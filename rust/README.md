# Shopping Sessions in Rust

This is a port of the C++ pipeline in `../src` to Rust. It uses the same algorithms and data layouts, so its output is identical and its speed is about the same. The README one level up explains how the pipeline works. All of the code was written for this event, under the MIT License.

It has no dependencies. The few libc calls it needs (`mmap`, `madvise`, `sysconf`, `getrusage`) are declared directly. So it builds offline into a static binary, about 0.7 MB, on an empty image.

## Build and run

```sh
docker build --platform linux/amd64 -t shopping-sessions-rs .
docker run --rm --platform linux/amd64 --network none --read-only --user 1000 \
  --cpus 8 -e NCPU=8 -v "$DATA:/data:ro" -v "$OUT:/output" shopping-sessions-rs
```

The binary takes the same arguments and environment variables as the C++ one: `DATA_DIR` and `OUTPUT_DIR`, or the last two arguments, plus `NCPU` and `SHOPPING_PROFILE`. The test switches carry over too: `SHOPPING_WIDE`, `SHOPPING_MMAP`, `SHOPPING_EXACT_IDS` and `SHOPPING_LONG_SLOTS`. `cargo build --features collide` makes long-ID keys collide on purpose, to test the restart in exact mode.

## How it maps to the C++

| C++ | Rust |
| --- | --- |
| `base.hpp` | `base.rs`: keys, the long-ID table, hashing, `run_tasks` on `std::thread::scope` |
| `memory.hpp` | `memory.rs`: inputs, `#[global_allocator]` with huge-page mappings for 1 MB and up, arenas, scatter buffers |
| `text.hpp` | `text.rs`: AVX2 and NEON separator masks through `std::arch`, timestamp and price parsing |
| `table.hpp` | `table.rs` |
| `sessions.hpp` | `sessions.rs`: an `Ev` trait for the u64 and u128 event packings |
| `parquet.hpp` | `parquet.rs`: `writev` through `write_vectored` |
| `main.cpp` | `main.rs` |

The code is `unsafe` only where the C++ works on raw memory:

- fixed-width loads that read into the padding after a field
- the packed 8-byte records
- the lock-free long-ID table
- workers that write disjoint parts of a shared array, through `Shared`

Rust doesn't allow a reference to a field of a packed record, so the C++ entry's misaligned-reference bug on x86 can't recur here.

Big allocations come from fresh zero mappings, so `vec![0; n]` does no memset.

## Checks

The port matches the official seed-1 dev answer for the new ID format in every mode: default, wide, mmap, exact IDs, one thread, the table-full restart and the forced-collision restart. It passes all 36 fixtures in each of those modes, and again through the amd64 image run with the scorer's flags. On generated seeds 2 to 4 (3M events each) and seed 7 (20M), it matches the C++ binary row for row.

On the 20M seed in Docker on an Apple M5 with 8 CPUs, the median run takes about 1.19 s, against 1.21 s for the C++ build in the same interleaved runs.
