use crate::{
    Cursor, Error, Execution, ExecutionState, Observation, ObserveRequest, OutputChunk,
    OutputStream, Result, ReturnReason,
};
use std::collections::VecDeque;

/// A bounded byte journal. Positions never move backwards when old bytes expire.
pub(crate) struct Journal {
    chunks: VecDeque<OutputChunk>,
    start: u64,
    end: u64,
    capacity: usize,
    pub revision: u64,
}

impl Journal {
    pub fn new(capacity: usize) -> Self {
        Self {
            chunks: VecDeque::new(),
            start: 0,
            end: 0,
            capacity,
            revision: 0,
        }
    }

    pub fn append(&mut self, stream: OutputStream, data: Vec<u8>) {
        self.end += data.len() as u64;
        self.chunks.push_back(OutputChunk { stream, data });
        let target = self.end.saturating_sub(self.capacity as u64);
        while self.start < target {
            let front = self.chunks.front_mut().unwrap();
            let discard = ((target - self.start) as usize).min(front.data.len());
            self.start += discard as u64;
            if discard == front.data.len() {
                self.chunks.pop_front();
            } else {
                front.data.drain(..discard);
            }
        }
    }

    pub fn read(
        &self,
        execution: &Execution,
        request: &ObserveRequest,
        limit: usize,
    ) -> Result<Observation> {
        let cursor = request.after_cursor;
        if let Some(cursor) = cursor
            && (cursor.handle != execution.handle
                || cursor.offset > self.end
                || cursor.revision > self.revision)
        {
            return Err(Error::invalid(
                "cursor does not belong to this execution or is ahead of its output",
            ));
        }
        let requested = cursor.map_or(0, |c| c.offset);
        let begin = requested.max(self.start);
        let end = self.end.min(begin.saturating_add(limit as u64));
        let mut position = self.start;
        let mut output = Vec::new();
        for chunk in &self.chunks {
            let next = position + chunk.data.len() as u64;
            let from = begin.max(position);
            let to = end.min(next);
            if from < to {
                output.push(OutputChunk {
                    stream: chunk.stream,
                    data: chunk.data[(from - position) as usize..(to - position) as usize].to_vec(),
                });
            }
            position = next;
            if position >= end {
                break;
            }
        }
        let has_more = end < self.end;
        let output_gap = requested < self.start;
        let return_reason = if end - begin >= limit as u64 {
            ReturnReason::OutputLimit
        } else if execution.state == ExecutionState::Finished {
            ReturnReason::Finished
        } else if !output.is_empty()
            || output_gap
            || cursor.is_none_or(|c| c.revision < self.revision)
        {
            ReturnReason::Activity
        } else {
            ReturnReason::WaitElapsed
        };
        Ok(Observation {
            execution: execution.clone(),
            output,
            next_cursor: Cursor {
                handle: execution.handle,
                offset: end,
                revision: self.revision,
            },
            has_more,
            output_gap,
            return_reason,
        })
    }
}
