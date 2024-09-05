use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        assert!(
            snapshot.l0_sstables.is_empty(),
            "should not add l0 ssts in tiered compaction"
        );

        if snapshot.levels.len() <= self.options.num_tiers {
            return None;
        }

        // 1. space amplification ratio
        let mut size = 0;
        for id in 0..(snapshot.levels.len() - 1) {
            size += snapshot.levels[id].1.len();
        }

        let space_amplification_ratio =
            (size as f64) / (snapshot.levels.last().unwrap().1.len() as f64);
        if space_amplification_ratio > self.options.max_size_amplification_percent as f64 {
            println!(
                "compaction triggered by space amplification ratio: {}",
                space_amplification_ratio
            );

            return Some(TieredCompactionTask {
                tiers: snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }

        // 2. size ratio
        let size_ratio_trigger = (self.options.size_ratio as f64 + 100.) / 100.;
        let mut size = 0;
        for i in 0..(snapshot.levels.len() - 1) {
            size += snapshot.levels[i].1.len();
            let next_level_size = snapshot.levels[i + 1].1.len();

            let size_ratio = (size as f64) / (next_level_size as f64);
            if size_ratio >= size_ratio_trigger && i + 2 >= self.options.min_merge_width {
                println!("compaction triggered by size ratio: {}", size_ratio);

                return Some(TieredCompactionTask {
                    tiers: snapshot
                        .levels
                        .iter()
                        .take(i + 2)
                        .cloned()
                        .collect::<Vec<_>>(),
                    bottom_tier_included: i + 2 >= snapshot.levels.len(),
                });
            }
        }

        // 3. reduce sorted runs
        let num_tiers_to_take = snapshot.levels.len() - self.options.num_tiers + 2;
        println!("compaction triggered by reducing sorted runs");
        let task = TieredCompactionTask {
            tiers: snapshot
                .levels
                .iter()
                .take(num_tiers_to_take)
                .cloned()
                .collect::<Vec<_>>(),
            bottom_tier_included: num_tiers_to_take <= snapshot.levels.len(),
        };

        Some(task)
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &TieredCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        assert!(
            snapshot.l0_sstables.is_empty(),
            "should not add l0 ssts in tiered compaction"
        );
        let mut snapshot = snapshot.clone();
        let mut files_to_remove = vec![];
        let mut levels = vec![];
        let mut new_tier_added = false;

        let mut tier_to_remove = task
            .tiers
            .iter()
            .map(|(x, y)| (*x, y))
            .collect::<HashMap<_, _>>();

        for (tier_id, sstables) in &snapshot.levels {
            if let Some(files) = tier_to_remove.remove(tier_id) {
                assert_eq!(
                    files, sstables,
                    "file changed after issuing compaction task"
                );
                files_to_remove.extend(sstables);
            } else {
                levels.push((*tier_id, sstables.clone()));
            }

            if tier_to_remove.is_empty() && !new_tier_added {
                new_tier_added = true;
                levels.push((output[0], output.to_vec()));
            }
        }

        if !tier_to_remove.is_empty() {
            unreachable!("some tiers not found");
        }

        snapshot.levels = levels;
        (snapshot, files_to_remove)
    }
}
