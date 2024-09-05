#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

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

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
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
    fn compact_generate_sst_from_iter(
        &self,
        mut iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
        compact_to_bottom_level: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut new_ssts = vec![];
        let mut builder = SsTableBuilder::new(self.options.block_size);

        while iter.is_valid() {
            if compact_to_bottom_level {
                if !iter.value().is_empty() {
                    builder.add(iter.key(), iter.value());
                }
            } else {
                builder.add(iter.key(), iter.value());
            }

            if builder.estimated_size() >= self.options.target_sst_size {
                let sst_id = self.next_sst_id();
                let table = builder.build(
                    sst_id,
                    Some(self.block_cache.clone()),
                    self.path_of_sst(sst_id),
                )?;
                new_ssts.push(Arc::new(table));
                builder = SsTableBuilder::new(self.options.block_size);
            }

            iter.next()?;
        }

        let sst_id = self.next_sst_id();
        let table = builder.build(
            sst_id,
            Some(self.block_cache.clone()),
            self.path_of_sst(sst_id),
        )?;
        new_ssts.push(Arc::new(table));

        Ok(new_ssts)
    }

    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        match task {
            CompactionTask::Leveled(_) => todo!(),
            CompactionTask::Tiered(task) => {
                let snapshot = self.state.read();
                let mut sst_concat_iter = vec![];

                for entry in task.tiers.iter() {
                    sst_concat_iter.push(Box::new(SstConcatIterator::create_and_seek_to_first(
                        entry
                            .1
                            .iter()
                            .map(|id| snapshot.sstables[id].clone())
                            .collect(),
                    )?));
                }
                let sstables = self.compact_generate_sst_from_iter(
                    MergeIterator::create(sst_concat_iter),
                    task.bottom_tier_included,
                )?;

                Ok(sstables)
            }
            CompactionTask::Simple(task) => {
                if task.upper_level.is_some() {
                    let iters = {
                        let guard = self.state.read();

                        let upper_iter = SstConcatIterator::create_and_seek_to_first(
                            task.upper_level_sst_ids
                                .iter()
                                .map(|id| guard.sstables[id].clone())
                                .collect(),
                        )?;
                        let lower_iter = SstConcatIterator::create_and_seek_to_first(
                            task.lower_level_sst_ids
                                .iter()
                                .map(|id| guard.sstables[id].clone())
                                .collect(),
                        )?;

                        TwoMergeIterator::create(upper_iter, lower_iter)?
                    };
                    self.compact_generate_sst_from_iter(iters, task.is_lower_level_bottom_level)
                } else {
                    let iters = {
                        let guard = self.state.read();

                        let upper_iter = MergeIterator::create(
                            task.upper_level_sst_ids
                                .iter()
                                .map(|id| {
                                    Box::new(
                                        SsTableIterator::create_and_seek_to_first(
                                            guard.sstables[id].clone(),
                                        )
                                        .expect("cannot create sstable iterator"),
                                    )
                                })
                                .collect::<Vec<_>>(),
                        );

                        let lower_iter = SstConcatIterator::create_and_seek_to_first(
                            task.lower_level_sst_ids
                                .iter()
                                .map(|id| guard.sstables[id].clone())
                                .collect(),
                        )?;

                        TwoMergeIterator::create(upper_iter, lower_iter)?
                    };

                    self.compact_generate_sst_from_iter(iters, false)
                }
            }
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let ssts = {
                    let guard = self.state.read();

                    l0_sstables
                        .iter()
                        .chain(l1_sstables)
                        .map(|id| {
                            Box::new(
                                SsTableIterator::create_and_seek_to_first(
                                    guard.sstables[id].clone(),
                                )
                                .expect("cannot create sstable iterator"),
                            )
                        })
                        .collect::<Vec<_>>()
                };
                let ssts_iter = MergeIterator::create(ssts);
                self.compact_generate_sst_from_iter(ssts_iter, true)
            }
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let (l0_sstables, l1_sstables) = {
            let guard = self.state.read();
            (guard.l0_sstables.clone(), guard.levels[0].1.clone())
        };

        let new_ssts = self.compact(&CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sstables.clone(),
            l1_sstables: l1_sstables.clone(),
        })?;

        let mut ssts_to_compact = vec![];

        {
            let _state_lock = self.state_lock.lock();
            let mut guard = self.state.write();
            let mut state = guard.as_ref().clone();

            state.l0_sstables.retain(|id| !l0_sstables.contains(id));

            for id in l0_sstables {
                if let Some(table) = state.sstables.remove(&id) {
                    ssts_to_compact.push(table);
                }
            }

            for id in l1_sstables {
                if let Some(table) = state.sstables.remove(&id) {
                    ssts_to_compact.push(table);
                }
            }

            let mut ids = vec![];
            for table in new_ssts {
                ids.push(table.sst_id());
                state.sstables.insert(table.sst_id(), table);
            }
            state.levels[0] = (0, ids);

            *guard = Arc::new(state);
        }

        for table in ssts_to_compact {
            std::fs::remove_file(self.path_of_sst(table.sst_id()))?;
        }

        Ok(())
    }

    fn simple_leveled_compaction(&self) {}

    fn trigger_compaction(&self) -> Result<()> {
        let snapshot = {
            let guard = self.state.read();
            guard.clone()
        };

        let task = self
            .compaction_controller
            .generate_compaction_task(&snapshot);
        let Some(task) = task else {
            return Ok(());
        };

        println!("running compaction task: {:?}", task);
        let sstables = self.compact(&task)?;
        let output = sstables
            .iter()
            .map(|table| table.sst_id())
            .collect::<Vec<_>>();

        let ssts_to_remove = {
            let _state_lock = self.state_lock.lock();
            let mut snapshot = self.state.read().as_ref().clone();
            let mut new_sst_ids = vec![];

            for file_to_add in sstables {
                new_sst_ids.push(file_to_add.sst_id());
                let result = snapshot.sstables.insert(file_to_add.sst_id(), file_to_add);
                assert!(result.is_none());
            }

            let (mut snapshot, files_to_remove) = self
                .compaction_controller
                .apply_compaction_result(&snapshot, &task, &output, false);

            let mut ssts_to_remove = Vec::with_capacity(files_to_remove.len());
            for file_to_remove in &files_to_remove {
                let result = snapshot.sstables.remove(file_to_remove);
                assert!(result.is_some(), "cannot remove {}.sst", file_to_remove);
                ssts_to_remove.push(result.unwrap());
            }
            let mut state = self.state.write();
            *state = Arc::new(snapshot);
            drop(state);
            ssts_to_remove
        };

        println!(
            "compaction finished: {} files removed, {} files added, output={:?}",
            ssts_to_remove.len(),
            output.len(),
            output
        );
        for sst in ssts_to_remove {
            std::fs::remove_file(self.path_of_sst(sst.sst_id()))?;
        }

        Ok(())
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
        let num_memtable_limit = {
            let guard = self.state.read();
            guard.imm_memtables.len() + 1
        };

        if num_memtable_limit > self.options.num_memtable_limit {
            self.force_flush_next_imm_memtable()?;
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
