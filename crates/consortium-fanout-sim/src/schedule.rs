//! Replayable completion order at DAG task boundaries.

use consortium::dag::{CompletionOrder, DagError, Result, TaskId};
use rand::Rng;
use rand_chacha::ChaCha8Rng;

use crate::fixtures::rng_from_seed;

/// One run's completion schedule, independent of the network fixture's RNG.
/// Capture [`Self::trace`] after [`consortium::dag::DagExecutor::run_simulated`]
/// and pass the IDs to [`Self::replay`] with a fresh DAG and simulator.
/// Choices order whole dispatched tasks, not events inside a task body.
pub struct CompletionSchedule {
    rng: Option<ChaCha8Rng>,
    choices: Vec<String>,
    consumed: usize,
}

impl CompletionSchedule {
    /// Generate choices among admitted tasks and record their exact IDs.
    pub fn seeded(seed: u64) -> Self {
        Self {
            rng: Some(rng_from_seed(seed)),
            choices: Vec::new(),
            consumed: 0,
        }
    }

    /// Require exactly these completions, including forced singleton choices.
    pub fn replay(choices: Vec<String>) -> Self {
        Self {
            rng: None,
            choices,
            consumed: 0,
        }
    }

    /// The prefix actually selected, excluding unconsumed replay input.
    pub fn trace(&self) -> &[String] {
        &self.choices[..self.consumed]
    }
}

impl CompletionOrder for CompletionSchedule {
    fn choose(&mut self, candidates: &[&TaskId]) -> Result<usize> {
        let step = self.consumed;
        if candidates.is_empty() {
            return Err(DagError::General(format!(
                "no dispatched tasks at step {step}"
            )));
        }
        let index = if let Some(rng) = &mut self.rng {
            let index = if candidates.len() == 1 {
                0
            } else {
                rng.gen_range(0..candidates.len() as u64) as usize
            };
            self.choices.push(candidates[index].0.clone());
            index
        } else {
            let next = self.choices.get(step).ok_or_else(|| {
                DagError::General(format!(
                    "trace ended at step {step}; dispatched: {candidates:?}"
                ))
            })?;
            candidates
                .binary_search_by(|id| id.0.cmp(next))
                .map_err(|_| {
                    DagError::General(format!(
                        "cannot complete {next:?} at step {step}; dispatched: {candidates:?}"
                    ))
                })?
        };
        self.consumed += 1;
        Ok(index)
    }

    fn finish(&mut self) -> Result<()> {
        if let Some(next) = self.choices.get(self.consumed) {
            return Err(DagError::General(format!(
                "DAG ended at step {}; extra completion {next:?}",
                self.consumed
            )));
        }
        Ok(())
    }
}
