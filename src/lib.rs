//! # oxide-raft-log
//!
//! A Raft-style replicated log for GPU cluster state with ternary entry status:
//! - `+1` = committed
//! - `0`  = appended
//! - `-1` = conflicting
//!
//! Features term-based conflict resolution, commit advancement via majority quorum,
//! log compaction, and leader election state tracking.

use std::collections::HashMap;

/// Ternary status for a log entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryStatus {
    /// Entry conflicts with existing state (or is rejected).
    Conflicting = -1,
    /// Entry has been appended but not yet committed.
    Appended = 0,
    /// Entry is committed (replicated to a majority).
    Committed = 1,
}

impl EntryStatus {
    pub fn value(&self) -> i8 {
        match self {
            EntryStatus::Conflicting => -1,
            EntryStatus::Appended => 0,
            EntryStatus::Committed => 1,
        }
    }
}

/// A single log entry in the replicated log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub index: u64,
    pub term: u64,
    pub command: Vec<u8>,
    pub status: EntryStatus,
}

impl LogEntry {
    pub fn new(index: u64, term: u64, command: Vec<u8>) -> Self {
        Self {
            index,
            term,
            command,
            status: EntryStatus::Appended,
        }
    }
}

/// Leader election state for a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeState {
    Follower,
    Candidate,
    Leader,
}

/// Tracks replication state per peer for quorum calculations.
#[derive(Debug, Clone, Default)]
struct PeerState {
    /// Highest log index that the peer has acknowledged as appended.
    match_index: u64,
}

/// A Raft-style replicated log.
#[derive(Debug)]
pub struct ReplicatedLog {
    /// The log entries.
    entries: Vec<LogEntry>,
    /// Current term.
    current_term: u64,
    /// Index of the highest log entry known to be committed.
    commit_index: u64,
    /// This node's election state.
    node_state: NodeState,
    /// Number of nodes in the cluster (for quorum).
    cluster_size: usize,
    /// Per-peer replication state (for commit advancement).
    peer_states: HashMap<u64, PeerState>,
    /// Index offset: logical index of the first entry in `entries`.
    /// Used after compaction to keep indices correct.
    index_offset: u64,
}

impl ReplicatedLog {
    /// Create a new replicated log for a cluster of `cluster_size` nodes.
    pub fn new(cluster_size: usize) -> Self {
        Self {
            entries: Vec::new(),
            current_term: 0,
            commit_index: 0,
            node_state: NodeState::Follower,
            cluster_size,
            peer_states: HashMap::new(),
            index_offset: 1,
        }
    }

    /// Get the current term.
    pub fn current_term(&self) -> u64 {
        self.current_term
    }

    /// Get the commit index.
    pub fn commit_index(&self) -> u64 {
        self.commit_index
    }

    /// Get the node's election state.
    pub fn node_state(&self) -> NodeState {
        self.node_state
    }

    /// Set the node's election state.
    pub fn set_node_state(&mut self, state: NodeState) {
        self.node_state = state;
    }

    /// Advance to a new term. Transitions to follower if not already.
    pub fn advance_term(&mut self, term: u64) {
        if term > self.current_term {
            self.current_term = term;
            self.node_state = NodeState::Follower;
        }
    }

    /// Get the last log index (0 if empty).
    pub fn last_log_index(&self) -> u64 {
        if self.entries.is_empty() {
            0
        } else {
            self.index_offset + self.entries.len() as u64 - 1
        }
    }

    /// Get the last log term (0 if empty).
    pub fn last_log_term(&self) -> u64 {
        self.entries.last().map(|e| e.term).unwrap_or(0)
    }

    /// Convert a logical index to a physical position in the entries vec.
    /// Returns None if the index is outside the current log range.
    fn to_physical(&self, index: u64) -> Option<usize> {
        if index < self.index_offset {
            return None;
        }
        let pos = (index - self.index_offset) as usize;
        if pos < self.entries.len() {
            Some(pos)
        } else {
            None
        }
    }

    /// Append a new entry at the next index in the given term.
    /// Returns the new entry's index.
    pub fn append(&mut self, term: u64, command: Vec<u8>) -> u64 {
        let index = self.last_log_index() + 1;
        self.current_term = self.current_term.max(term);
        self.entries.push(LogEntry::new(index, term, command));
        index
    }

    /// Append an entry from a leader at a specific index.
    /// Performs conflict detection: if an existing entry at this index has a different
    /// (lower) term, it is overwritten (higher term wins). If the terms match, it's a
    /// duplicate append (idempotent). If the existing entry has a higher term, the append
    /// is rejected as conflicting.
    ///
    /// Returns the status of the operation.
    pub fn append_at(&mut self, index: u64, term: u64, command: Vec<u8>) -> EntryStatus {
        self.current_term = self.current_term.max(term);

        // If the index is beyond our log, we have a gap — reject.
        if index > self.last_log_index() + 1 {
            return EntryStatus::Conflicting;
        }

        let physical = self.to_physical(index);

        match physical {
            Some(pos) => {
                let existing = &self.entries[pos];
                if existing.term == term {
                    // Same term — idempotent, already appended.
                    EntryStatus::Appended
                } else if existing.term < term {
                    // Higher term wins — overwrite the conflicting entry and truncate
                    // everything after it.
                    self.entries.truncate(pos);
                    self.entries.push(LogEntry::new(index, term, command));
                    EntryStatus::Appended
                } else {
                    // Existing term is higher — reject.
                    EntryStatus::Conflicting
                }
            }
            None => {
                // Index is exactly last_log_index + 1 — normal append.
                if index == self.last_log_index() + 1 {
                    self.entries.push(LogEntry::new(index, term, command));
                    EntryStatus::Appended
                } else {
                    EntryStatus::Conflicting
                }
            }
        }
    }

    /// Record that a peer has appended up to `match_index`.
    pub fn record_peer_append(&mut self, peer_id: u64, match_index: u64) {
        let state = self.peer_states.entry(peer_id).or_default();
        state.match_index = state.match_index.max(match_index);
    }

    /// Advance the commit index based on majority replication.
    /// An index is committed when a majority of nodes have appended it.
    /// Returns the new commit index.
    pub fn advance_commit(&mut self) -> u64 {
        // Count how many nodes (including self) have each index.
        // self.last_log_index() is our own match index.
        let self_match = self.last_log_index();

        // Collect all match indices.
        let mut match_indices: Vec<u64> = self
            .peer_states
            .values()
            .map(|p| p.match_index)
            .collect();
        match_indices.push(self_match);

        // Sort descending to find the N-th highest (quorum threshold).
        match_indices.sort_by(|a, b| b.cmp(a));

        // Quorum: majority of cluster_size.
        let quorum = self.cluster_size / 2 + 1;
        // The index at position quorum-1 is replicated by at least quorum nodes.
        if match_indices.len() >= quorum {
            let new_commit = match_indices[quorum - 1];
            if new_commit > self.commit_index {
                // Only commit entries from the current leader's term or verify
                // they exist in our log.
                if let Some(pos) = self.to_physical(new_commit) {
                    if self.entries[pos].term <= self.current_term {
                        self.commit_index = new_commit;
                    }
                }
            }
        }

        self.commit_index
    }

    /// Commit a specific index directly (e.g., from a leader's directive).
    /// Returns true if the index was successfully committed.
    pub fn commit(&mut self, index: u64) -> bool {
        if let Some(pos) = self.to_physical(index) {
            self.commit_index = self.commit_index.max(index);
            // Mark the entry and all prior entries as committed.
            for entry in &mut self.entries[..=pos] {
                if entry.status == EntryStatus::Appended {
                    entry.status = EntryStatus::Committed;
                }
            }
            true
        } else {
            false
        }
    }

    /// Compact the log by truncating all entries up to (and including) `through_index`.
    /// Typically called for entries that are already committed.
    /// Returns the number of entries removed.
    pub fn compact(&mut self, through_index: u64) -> usize {
        if through_index < self.index_offset {
            return 0;
        }
        let physical = self.to_physical(through_index);
        match physical {
            Some(pos) => {
                let removed = pos + 1;
                self.entries.drain(..=pos);
                self.index_offset = through_index + 1;
                removed
            }
            None => 0,
        }
    }

    /// Get a reference to an entry by index.
    pub fn get(&self, index: u64) -> Option<&LogEntry> {
        self.to_physical(index).map(|pos| &self.entries[pos])
    }

    /// Get the number of entries currently in the log.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if the log is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Get all entries from `start_index` onwards.
    pub fn entries_from(&self, start_index: u64) -> Vec<&LogEntry> {
        match self.to_physical(start_index) {
            Some(pos) => self.entries[pos..].iter().collect(),
            None => Vec::new(),
        }
    }

    /// Start an election: transition to candidate, increment term, vote for self.
    pub fn start_election(&mut self) -> u64 {
        self.current_term += 1;
        self.node_state = NodeState::Candidate;
        self.current_term
    }

    /// Become leader after winning election.
    pub fn become_leader(&mut self) {
        self.node_state = NodeState::Leader;
    }

    /// Check if a candidate's log is at least as up-to-date as ours.
    /// Used to determine vote eligibility.
    pub fn is_log_up_to_date(&self, last_term: u64, last_index: u64) -> bool {
        if self.last_log_term() != last_term {
            last_term > self.last_log_term()
        } else {
            last_index >= self.last_log_index()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_append_and_index() {
        let mut log = ReplicatedLog::new(3);
        let idx1 = log.append(1, b"cmd1".to_vec());
        let idx2 = log.append(1, b"cmd2".to_vec());
        let idx3 = log.append(2, b"cmd3".to_vec());

        assert_eq!(idx1, 1);
        assert_eq!(idx2, 2);
        assert_eq!(idx3, 3);
        assert_eq!(log.len(), 3);
        assert_eq!(log.last_log_index(), 3);
        assert_eq!(log.last_log_term(), 2);
    }

    #[test]
    fn test_entry_status_ternary() {
        assert_eq!(EntryStatus::Conflicting.value(), -1);
        assert_eq!(EntryStatus::Appended.value(), 0);
        assert_eq!(EntryStatus::Committed.value(), 1);
    }

    #[test]
    fn test_append_at_conflict_detection_higher_term_wins() {
        let mut log = ReplicatedLog::new(3);
        log.append(1, b"original".to_vec());

        // Overwrite with higher term — should succeed.
        let status = log.append_at(1, 3, b"overwrite".to_vec());
        assert_eq!(status, EntryStatus::Appended);
        assert_eq!(log.get(1).unwrap().term, 3);
        assert_eq!(log.get(1).unwrap().command, b"overwrite".to_vec());
    }

    #[test]
    fn test_append_at_conflict_detection_lower_term_rejected() {
        let mut log = ReplicatedLog::new(3);
        log.append(5, b"original".to_vec());

        // Try to append with lower term — should be rejected.
        let status = log.append_at(1, 2, b"reject".to_vec());
        assert_eq!(status, EntryStatus::Conflicting);
        assert_eq!(log.get(1).unwrap().term, 5);
    }

    #[test]
    fn test_append_at_idempotent_same_term() {
        let mut log = ReplicatedLog::new(3);
        log.append(1, b"original".to_vec());

        let status = log.append_at(1, 1, b"duplicate".to_vec());
        assert_eq!(status, EntryStatus::Appended);
        // Original should be preserved (idempotent).
        assert_eq!(log.get(1).unwrap().command, b"original".to_vec());
    }

    #[test]
    fn test_commit_advancement_with_quorum() {
        let mut log = ReplicatedLog::new(5); // 5 nodes, quorum = 3
        log.append(1, b"a".to_vec()); // index 1
        log.append(1, b"b".to_vec()); // index 2
        log.append(1, b"c".to_vec()); // index 3

        // Self has appended all 3. Peers have partial.
        log.record_peer_append(2, 3); // peer 2 has all
        log.record_peer_append(3, 2); // peer 3 has up to 2
        log.record_peer_append(4, 1); // peer 4 has up to 1

        let new_commit = log.advance_commit();
        // 3 nodes (self + peer 2 + peer 3) have at least index 2 → commit advances to 2.
        assert_eq!(new_commit, 2);
    }

    #[test]
    fn test_compaction_removes_committed_prefix() {
        let mut log = ReplicatedLog::new(3);
        log.append(1, b"a".to_vec()); // index 1
        log.append(1, b"b".to_vec()); // index 2
        log.append(1, b"c".to_vec()); // index 3

        // Commit first two entries.
        log.commit(2);

        // Compact through index 2.
        let removed = log.compact(2);
        assert_eq!(removed, 2);
        assert_eq!(log.len(), 1);
        assert!(log.get(1).is_none());
        assert!(log.get(2).is_none());
        assert_eq!(log.get(3).unwrap().command, b"c".to_vec());
        assert_eq!(log.last_log_index(), 3);
    }

    #[test]
    fn test_leader_election_state_transitions() {
        let mut log = ReplicatedLog::new(3);

        assert_eq!(log.node_state(), NodeState::Follower);

        // Start election.
        let new_term = log.start_election();
        assert_eq!(log.node_state(), NodeState::Candidate);
        assert_eq!(new_term, 1);
        assert_eq!(log.current_term(), 1);

        // Win election.
        log.become_leader();
        assert_eq!(log.node_state(), NodeState::Leader);

        // Discover higher term from another leader.
        log.advance_term(5);
        assert_eq!(log.node_state(), NodeState::Follower);
        assert_eq!(log.current_term(), 5);
    }

    #[test]
    fn test_log_up_to_date_check() {
        let mut log = ReplicatedLog::new(3);
        log.append(2, b"x".to_vec()); // index 1, term 2
        log.append(2, b"y".to_vec()); // index 2, term 2

        // Candidate with same last term but shorter log → not up to date.
        assert!(!log.is_log_up_to_date(2, 1));

        // Candidate with same last term and same/longer log → up to date.
        assert!(log.is_log_up_to_date(2, 2));
        assert!(log.is_log_up_to_date(2, 3));

        // Candidate with higher last term → up to date regardless.
        assert!(log.is_log_up_to_date(3, 1));

        // Candidate with lower last term → not up to date.
        assert!(!log.is_log_up_to_date(1, 5));
    }

    #[test]
    fn test_append_at_truncates_on_conflict() {
        let mut log = ReplicatedLog::new(3);
        log.append(1, b"a".to_vec()); // index 1
        log.append(1, b"b".to_vec()); // index 2
        log.append(1, b"c".to_vec()); // index 3

        // Append at index 2 with higher term — should truncate index 2 and 3.
        let status = log.append_at(2, 3, b"b-new".to_vec());
        assert_eq!(status, EntryStatus::Appended);
        assert_eq!(log.len(), 2);
        assert_eq!(log.get(2).unwrap().command, b"b-new".to_vec());
        assert!(log.get(3).is_none());
    }
}
