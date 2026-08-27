use super::{AppendEntriesRequest, MAX_APPEND_ENTRIES, RaftConsensus};
use crate::peer::PeerId;
use crate::peer::consensus::{ConsensusEffect, RaftLogEntry};
use crate::storage::BlockHash;
use std::time::Instant;

impl RaftConsensus {
    pub(super) fn handle_append_entries(
        &mut self,
        request: AppendEntriesRequest,
    ) -> Result<Vec<ConsensusEffect>, String> {
        if !self.receive_append_entries_at(
            request.term,
            request.leader_id,
            request.from,
            request.now,
        ) {
            return Ok(vec![self.append_entries_response(
                request.from,
                false,
                self.matching_index_for(request.prev_log_index, request.prev_log_term, &[]),
            )]);
        }

        let (log_entries, mut blocks) = match Self::validate_replicated_entries(request.entries) {
            Ok(validated) => validated,
            Err(e) => {
                log::warn!("Rejecting Raft append entries from {}: {e}", request.from);
                let match_index =
                    self.matching_index_for(request.prev_log_index, request.prev_log_term, &[]);
                return Ok(vec![self.append_entries_response(
                    request.from,
                    false,
                    match_index,
                )]);
            }
        };

        let match_index =
            self.matching_index_for(request.prev_log_index, request.prev_log_term, &log_entries);
        let accepted_match_index = log_entries
            .last()
            .map(|entry| entry.index)
            .unwrap_or(request.prev_log_index);
        let log_changed = accepted_match_index > request.prev_log_index;
        let previous_commit_index = self.commit_index;
        let Some(mut effects) = self.append_entries(
            request.prev_log_index,
            request.prev_log_term,
            log_entries,
            request.leader_commit,
        ) else {
            return Ok(vec![self.append_entries_response(
                request.from,
                false,
                match_index,
            )]);
        };

        for effect in &effects {
            if let ConsensusEffect::StageRaftEntries(entries) = effect {
                for entry in entries {
                    if !entry.is_noop()
                        && let Some(block_file) = blocks.remove(&entry.block_hash)
                    {
                        self.pending_blocks.insert(entry.block_hash, block_file);
                    }
                }
            }
        }

        // The ack is appended last, not first: `PeerEffects::execute_all` runs effects in
        // order and stops at the first error, so the leader only learns this follower
        // replicated successfully once staging/commit/rollback have actually executed.
        effects.push(self.append_entries_response(request.from, true, accepted_match_index));
        if log_changed || self.commit_index != previous_commit_index {
            self.persist_log()?;
        }
        Ok(effects)
    }

    pub(super) fn append_entries_response(
        &self,
        to: PeerId,
        success: bool,
        match_index: u64,
    ) -> ConsensusEffect {
        ConsensusEffect::Send {
            to,
            body: crate::peer::MessageBody::RaftAppendEntriesResponse {
                term: self.current_term,
                success,
                match_index,
            },
        }
    }

    pub(super) fn append_entries(
        &mut self,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<RaftLogEntry>,
        leader_commit: u64,
    ) -> Option<Vec<ConsensusEffect>> {
        if self.term_at(prev_log_index) != Some(prev_log_term) {
            return None;
        }

        if !entries_are_contiguous_after(prev_log_index, &entries) {
            return None;
        }

        let previous_commit_index = self.commit_index;
        let entries_to_stage: Vec<_> = entries
            .iter()
            .copied()
            .filter(|entry| entry.index > previous_commit_index && !entry.is_noop())
            .collect();

        let mut discarded_block_hashes = Vec::new();
        for entry in entries {
            if let Some(existing) = self.log_entry(entry.index) {
                if existing.term != entry.term {
                    // The leader is safe to trust once the log-matching check above has
                    // passed, but a conflicting suffix must never reach into already
                    // committed history.
                    if entry.index <= self.commit_index {
                        return None;
                    }
                    let truncate_at = usize::try_from(entry.index - 1)
                        .expect("Raft log index does not fit in usize");
                    discarded_block_hashes.extend(
                        self.log
                            .iter()
                            .skip(truncate_at)
                            .filter(|discarded| !discarded.is_noop())
                            .map(|discarded| discarded.block_hash),
                    );
                    self.log.truncate(truncate_at);
                    self.log.push(entry);
                }
            } else if entry.index == self.last_log_index() + 1 {
                self.log.push(entry);
            }
        }

        self.commit_index = self
            .commit_index
            .max(leader_commit.min(self.last_log_index()));
        let mut effects: Vec<ConsensusEffect> = discarded_block_hashes
            .into_iter()
            .map(ConsensusEffect::RollbackBlock)
            .collect();
        if !entries_to_stage.is_empty() {
            effects.push(ConsensusEffect::StageRaftEntries(entries_to_stage));
        }
        effects.extend(
            self.log
                .iter()
                .filter(|entry| {
                    entry.index > previous_commit_index && entry.index <= self.commit_index
                })
                .filter(|entry| !entry.is_noop())
                .map(|entry| ConsensusEffect::CommitBlock(entry.block_hash)),
        );
        Some(effects)
    }

    pub(super) fn last_log_index(&self) -> u64 {
        self.log.last().map(|entry| entry.index).unwrap_or(0)
    }

    pub(super) fn last_log_term(&self) -> u64 {
        self.log.last().map(|entry| entry.term).unwrap_or(0)
    }

    fn term_at(&self, index: u64) -> Option<u64> {
        if index == 0 {
            return Some(0);
        }
        self.log_entry(index).map(|entry| entry.term)
    }

    fn log_entry(&self, index: u64) -> Option<&RaftLogEntry> {
        self.log.iter().find(|entry| entry.index == index)
    }

    pub(super) fn matching_index_for(
        &self,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: &[RaftLogEntry],
    ) -> u64 {
        let previous_match = if self.term_at(prev_log_index) == Some(prev_log_term) {
            prev_log_index
        } else {
            0
        };

        entries
            .iter()
            .filter(|entry| self.term_at(entry.index) == Some(entry.term))
            .map(|entry| entry.index)
            .max()
            .unwrap_or(previous_match)
    }

    fn persist_log(&mut self) -> Result<(), String> {
        self.raft_log_store.save(&self.log, self.commit_index)
    }

    pub(super) fn heartbeat_due(&self, now: Instant) -> bool {
        self.last_heartbeat_sent_at
            .is_none_or(|last_sent_at| now.duration_since(last_sent_at) >= self.heartbeat_interval)
    }

    pub(super) fn handle_leader_append_entries_response(
        &mut self,
        term: u64,
        from: PeerId,
        success: bool,
        match_index: u64,
    ) -> Result<Vec<ConsensusEffect>, String> {
        if term > self.current_term {
            self.step_down(term);
            return Ok(Vec::new());
        }

        if term != self.current_term || !self.participants.contains(&from) {
            return Ok(Vec::new());
        }

        if success {
            let current_match_index = self.match_indexes.entry(from).or_insert(0);
            *current_match_index = (*current_match_index).max(match_index);
            let previous_commit_index = self.commit_index;
            let effects = self.advance_commit_index();
            if self.commit_index != previous_commit_index {
                self.persist_log()?;
            }
            return Ok(effects);
        }

        let match_index = match_index.min(self.last_log_index());
        self.match_indexes.insert(from, match_index);
        if match_index >= self.last_log_index() {
            return Ok(Vec::new());
        }

        Ok(self.append_entries_effect_for(from).into_iter().collect())
    }

    pub(super) fn append_local_block(
        &mut self,
        block_hash: BlockHash,
    ) -> Result<Vec<ConsensusEffect>, String> {
        self.ensure_no_uncommitted_block()?;
        let entry = RaftLogEntry {
            term: self.current_term,
            index: self.last_log_index() + 1,
            block_hash,
        };
        self.log.push(entry);
        self.match_indexes.insert(self.peer_id, entry.index);
        let mut effects = self.append_entries_effects_for_followers();
        effects.extend(self.advance_commit_index());
        self.persist_log()?;
        Ok(effects)
    }

    pub(super) fn append_leader_noop(&mut self) -> Result<Vec<ConsensusEffect>, String> {
        let entry = RaftLogEntry::noop(self.current_term, self.last_log_index() + 1);
        self.log.push(entry);
        self.match_indexes.insert(self.peer_id, entry.index);
        let mut effects = self.append_entries_effects_for_followers();
        effects.extend(self.advance_commit_index());
        self.persist_log()?;
        Ok(effects)
    }

    pub(super) fn ensure_no_uncommitted_block(&self) -> Result<(), String> {
        if self
            .log
            .iter()
            .any(|entry| entry.index > self.commit_index && !entry.is_noop())
        {
            return Err("Raft leader already has an uncommitted block".to_string());
        }
        Ok(())
    }

    fn advance_commit_index(&mut self) -> Vec<ConsensusEffect> {
        let previous_commit_index = self.commit_index;
        for entry in self.log.iter().rev() {
            if entry.index <= self.commit_index || entry.term != self.current_term {
                continue;
            }

            let replicated_count = self
                .participants
                .iter()
                .filter(|peer_id| {
                    self.match_indexes
                        .get(peer_id)
                        .is_some_and(|match_index| *match_index >= entry.index)
                })
                .count();
            if replicated_count >= self.majority() {
                self.commit_index = entry.index;
                break;
            }
        }

        self.log
            .iter()
            .filter(|entry| entry.index > previous_commit_index && entry.index <= self.commit_index)
            .filter(|entry| !entry.is_noop())
            .map(|entry| ConsensusEffect::CommitBlock(entry.block_hash))
            .collect()
    }

    pub(super) fn append_entries_effects_for_followers(&self) -> Vec<ConsensusEffect> {
        self.participants
            .iter()
            .copied()
            .filter(|peer_id| *peer_id != self.peer_id)
            .filter_map(|peer_id| self.append_entries_effect_for(peer_id))
            .collect()
    }

    fn append_entries_effect_for(&self, peer_id: PeerId) -> Option<ConsensusEffect> {
        let peer_match_index = self.match_indexes.get(&peer_id).copied().unwrap_or(0);
        let prev_log_index = peer_match_index.min(self.last_log_index());
        let prev_log_term = self.term_at(prev_log_index)?;
        let entries = self
            .log
            .iter()
            .filter(|entry| entry.index > prev_log_index)
            .take(MAX_APPEND_ENTRIES)
            .copied()
            .collect();

        Some(ConsensusEffect::SendRaftAppendEntries {
            to: peer_id,
            term: self.current_term,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit: self.commit_index,
        })
    }
}

fn entries_are_contiguous_after(prev_log_index: u64, entries: &[RaftLogEntry]) -> bool {
    entries
        .iter()
        .enumerate()
        .all(|(offset, entry)| entry.index == prev_log_index + offset as u64 + 1)
}
