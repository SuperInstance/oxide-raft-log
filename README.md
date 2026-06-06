# oxide-raft-log

Raft-style replicated log for GPU cluster state with ternary entry status. Term conflicts, commit advancement, log compaction.

## Overview

# oxide-raft-log

A Raft-style replicated log for GPU cluster state with ternary entry status:

## Architecture

This crate sits within the **five-layer Oxide Stack**:

| Layer | Crate | Role |
|-------|-------|------|
| 1 | open-parallel | Async runtime (tokio fork) |
| 2 | pincher | "Vector DB as runtime, LLM as compiler" |
| 3 | flux-core | Bytecode VM + A2A agent protocol |
| 4 | cuda-oxide | Flux→MIR→Pliron→NVVM→PTX compiler |
| 5 | cudaclaw | Persistent GPU kernels, warp consensus, SmartCRDT |

The key insight: **ternary values {-1, 0, +1} map directly to GPU compute**. They pack 16× denser than FP32, enable XNOR+popcount matmul, and conservation laws become compile-time checks.

## Stats

| Metric | Value |
|--------|-------|
| Tests | 10 |
| Lines of Code | 493 |
| Public API Surface | 27 items |
| License | MIT |

## Installation

```toml
[dependencies]
oxide-raft-log = "0.1.0"
```

## Usage

```rust
use oxide_raft_log::*;
// See src/lib.rs tests for complete working examples
```

### Key Types

```
- pub enum EntryStatus {
    pub fn value(&self) -> i8 {
- pub struct LogEntry {
    pub fn new(index: u64, term: u64, command: Vec<u8>) -> Self {
- pub enum NodeState {
- pub struct ReplicatedLog {
    pub fn new(cluster_size: usize) -> Self {
    pub fn current_term(&self) -> u64 {
    pub fn commit_index(&self) -> u64 {
    pub fn node_state(&self) -> NodeState {
```

## Design Philosophy

This crate uses **ternary algebra** (Z₃) where every value is {-1, 0, +1}:

- **+1** → positive signal (healthy, allocated, converged, ready)
- **0** → neutral (pending, balanced, monitoring, degraded)
- **-1** → negative signal (failed, free, diverged, overloaded)

This isn't arbitrary — ternary is the natural encoding for:
1. **BitNet b1.58** (Microsoft) — ternary neural networks at 60% less power
2. **GPU warp voting** — hardware ballot instructions return ternary consensus
3. **Conservation laws** — {-1, 0, +1} preserves quantity (what goes in must come out)

## Testing

```bash
git clone https://github.com/SuperInstance/oxide-raft-log.git
cd oxide-raft-log
cargo test
```

## License

MIT
