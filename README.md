# Oxide Raft Log

A **Raft-style replicated log** with ternary entry status — each log entry is in one of three states: `Committed (+1)`, `Appended (0)`, or `Conflicting (-1)`. This crate implements term-based conflict resolution, commit advancement via majority quorum, and log compaction for the SuperInstance GPU cluster.

## Why It Matters

Distributed consensus is the backbone of any fault-tolerant system. Raft (Ongaro & Ousterhout, 2014) achieves consensus through a leader-based replicated log. This crate adapts Raft for GPU cluster state management where entries aren't just "committed or not" — they have a *ternary* status that captures the full lifecycle including conflicts during leader changes. In a GPU cluster running heterogeneous workloads, configuration conflicts are common during failover. The ternary status lets the system reason about *why* an entry isn't committed: was it merely appended (0, still pending) or does it actively conflict (-1, superseded by a competing leader's term)?

## How It Works

### Raft Consensus Primer

Raft elects a **leader** that serializes all state changes. The leader appends entries to its log, replicates them to followers, and commits once a majority (quorum) acknowledge:

```
commit_index = max{ index : match_index[i] > commit_index for majority of i }
```

If the leader fails, candidates with up-to-date logs win elections. Conflicting entries from old terms are overwritten.

### Ternary Entry Status

Unlike standard Raft's binary committed/uncommitted, this crate uses:

| Status | Value | Meaning |
|--------|-------|---------|
| `Committed` | +1 | Replicated to majority, safe to apply |
| `Appended` | 0 | In the log but not yet replicated to quorum |
| `Conflicting` | -1 | Conflicts with a newer entry from a higher term |

During leader election, entries from different terms can collide. The ternary status preserves this information — a conflicting entry isn't just uncommitted, it's *actively wrong*. This maps to the {-1, 0, +1} ternary domain used throughout SuperInstance.

### Term-Based Conflict Resolution

When a new entry arrives with a higher term than an existing entry at the same index, the old entry's status becomes `Conflicting`:

```
if new_entry.term > existing.term at same index:
    existing.status = Conflicting
    replace with new_entry (status = Appended)
```

This is O(1) per entry and preserves audit history via the status field.

### Commit Advancement

The leader tracks `match_index` per peer. An entry is committed when:

```
|{ i : match_index[i] >= entry.index }| ≥ ⌊(cluster_size - 1) / 2⌋ + 1
```

Complexity: O(N) per commit check where N = cluster size. In practice, N ≤ 7, so this is negligible.

### Log Compaction

After snapshots, old entries are discarded. The `index_offset` tracks the logical index of the first remaining entry, so external references (e.g., "entry at index 42") still resolve correctly after compaction.

## Quick Start

```rust
use oxide_raft_log::{ReplicatedLog, EntryStatus, NodeState};

fn main() {
    let mut log = ReplicatedLog::new(5); // 5-node cluster

    // Append an entry in term 1
    let entry = log.append(1, b"set x = 42".to_vec());
    println!("Entry {} status: {:?}", entry.index, entry.status); // Appended

    // Simulate quorum: mark 3 peers as having matched
    log.record_peer_match(1, 1);
    log.record_peer_match(2, 1);
    log.advance_commit();
    println!("Commit index: {}", log.commit_index()); // 1

    // Check for conflicts
    log.advance_term(2);
    let conflicting = log.append(2, b"set x = 99".to_vec());
    println!("New entry term: {}", conflicting.term);
}
```

```bash
cargo build
cargo test
```

## API

| Type | Method | Description |
|------|--------|-------------|
| `ReplicatedLog` | `new(cluster_size)` | Create log for N-node cluster |
| `ReplicatedLog` | `append(term, command)` | Add entry (status = Appended) |
| `ReplicatedLog` | `advance_commit()` | Check quorum, advance commit_index |
| `ReplicatedLog` | `record_peer_match(peer_id, index)` | Update peer replication state |
| `ReplicatedLog` | `advance_term(term)` | Begin new term, revert to Follower |
| `ReplicatedLog` | `compact_up_to(index)` | Discard entries before index |
| `EntryStatus` | `Conflicting \| Appended \| Committed` | Ternary status enum |
| `NodeState` | `Follower \| Candidate \| Leader` | Election state |

## Architecture Notes

Oxide Raft Log provides the consensus substrate for SuperInstance's distributed state — the γ (gamma) of agreement. Agents *construct* shared state through log replication. The ternary entry status (Committed/Appended/Conflicting) maps directly to the {-1, 0, +1} domain, making it native to the γ + η = C framework: committed entries are γ (+1), conflicting entries are η (-1), and appended entries are the neutral transition state (0). See [ARCHITECTURE.md](https://github.com/SuperInstance/SuperInstance/blob/main/ARCHITECTURE.md).

## References

1. Ongaro, D., & Ousterhout, J. (2014). "In Search of an Understandable Consensus Algorithm." *USENIX ATC*. — The Raft paper.
2. Lamport, L. (1998). "The Part-Time Parliament." *ACM TOCS*, 16(2), 133–169. — Paxos, the predecessor Raft improves on.
3. Howard, H., Malkhi, D., & Spiegelman, A. (2017). "Flexible Paxos: Quorum intersection revisited." *arXiv:1608.06696*. — On relaxing quorum requirements.

## License

MIT
