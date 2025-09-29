// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod leveled;
mod simple_leveled;
mod tiered;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::block::BlockBuilder;
use crate::iterators::StorageIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact(&self, _task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        match _task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let snapshot = { self.state.read().clone() };
                let mut l0_ssts_to_compact = vec![];
                for table_id in l0_sstables.clone() {
                    if let Some(t) = snapshot.sstables.get(&table_id) {
                        l0_ssts_to_compact.push(Box::new(
                            SsTableIterator::create_and_seek_to_first(t.clone())?,
                        ));
                    }
                }
                let mut l1_ssts_to_compact = vec![];
                for table_id in l1_sstables.clone() {
                    if let Some(t) = snapshot.sstables.get(&table_id) {
                        l1_ssts_to_compact.push(Box::new(
                            SsTableIterator::create_and_seek_to_first(t.clone())?,
                        ));
                    }
                }
                let mut iter = TwoMergeIterator::create(
                    MergeIterator::create(l0_ssts_to_compact),
                    MergeIterator::create(l1_ssts_to_compact),
                )?;
                let mut sstable_builder = SsTableBuilder::new(self.options.block_size);
                let mut new_ssts = vec![];
                while iter.is_valid() {
                    if !iter.value().is_empty() {
                        sstable_builder.add(iter.key(), iter.value());
                    }
                    if sstable_builder.estimated_size() > self.options.target_sst_size {
                        let sst_id = self.next_sst_id();
                        new_ssts.push(Arc::new(sstable_builder.build(
                            sst_id,
                            Some(self.block_cache.clone()),
                            self.path_of_sst(sst_id),
                        )?));
                        sstable_builder = SsTableBuilder::new(self.options.block_size);
                    }
                    iter.next()?;
                }
                let sst_id = self.next_sst_id();
                new_ssts.push(Arc::new(sstable_builder.build(
                    sst_id,
                    Some(self.block_cache.clone()),
                    self.path_of_sst(sst_id),
                )?));
                Ok(new_ssts)
            }
            _ => {
                unimplemented!()
            }
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let (l0_sstables, l1_sstables) = {
            let read_state = self.state.read();
            let l0_sstables = read_state.l0_sstables.clone();
            let l1_sstables;
            if read_state.levels.len() > 0 {
                l1_sstables = read_state.levels[0].1.clone();
            } else {
                l1_sstables = vec![];
            }
            (l0_sstables, l1_sstables)
        };
        let task = CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sstables.clone(),
            l1_sstables: l1_sstables.clone(),
        };
        let compacted_ssts = self.compact(&task)?;
        {
            let state_lock = self.state_lock.lock();
            let mut guard = self.state.write();
            let mut current = guard.as_ref().clone();
            for table in &compacted_ssts {
                current.sstables.insert(table.sst_id(), table.clone());
            }
            let new_l0 = current
                .l0_sstables
                .iter()
                .filter(|t| !l0_sstables.contains(t))
                .map(|t| t.clone())
                .collect::<Vec<usize>>();
            current.l0_sstables = new_l0;
            let new_l1 = compacted_ssts.iter().map(|t| t.sst_id()).collect();
            if current.levels.len() < 1 {
                current.levels.push((0, new_l1));
            } else {
                current.levels[0] = (0, new_l1);
            }
            *guard = Arc::new(current);
            drop(guard);
        }
        for table_id in [l0_sstables, l1_sstables].concat() {
            match std::fs::remove_file(self.path_of_sst(table_id)) {
                Ok(()) => {}
                Err(e) => eprintln!("Couldn't remove sst {} {}", table_id, e.to_string()),
            };
        }
        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        unimplemented!()
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        if self.state.read().imm_memtables.len() >= self.options.num_memtable_limit {
            let _ = self.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
