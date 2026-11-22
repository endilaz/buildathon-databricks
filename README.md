# Databricks Shopping Sessions

This C++20 pipeline turns raw clickstream CSVs into the two exact daily Parquet tables for the Databricks buildathon track. It is a single static binary on an empty image, with no runtime dependencies. All of its code was written for this event. [Track overview](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions) · [Data specification](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions/files/rules.md?inline=1)

## Run the scored entry point

```sh
docker build --platform linux/amd64 -t shopping-sessions .
docker run --rm --platform linux/amd64 --network none --read-only --user 1000 \
  --cpus 8 -e NCPU=8 -v "$DATA:/data:ro" -v "$OUT:/output" shopping-sessions
```

It reads `DATA_DIR` and `OUTPUT_DIR` (default `/data` and `/output`), or the last two arguments. Set `SHOPPING_PROFILE=1` to print stage timings to stderr.

## How it works

IDs never live in memory as strings. An ID of up to 10 bytes from `[0-9A-Z_a-z]` is packed into a 64-bit integer, 6 bits per byte. Integer equality is string equality, and integer order is byte-wise string order, which the "smallest ID" and "smallest click_id" rules need. Any other ID is long: since the generator gives one in eight IDs a 32-digit hex suffix, that is about one ID in eight. A long ID is keyed by 63 bits of a hash of its bytes, computed where it is read, with no shared state. Only user, click and link IDs, whose bytes may be needed for names, ties or output, are also filed in one lock-free insert-only table, together with 16 more hash bits. Event IDs carry those 16 bits into dedup. So two distinct IDs that share a key are noticed, and the run restarts with `SHOPPING_EXACT_IDS=1`, which interns long IDs exactly. A table too full to take more restarts the run with a table four times the size. A person is represented by the key of its smallest ID, so no step ever builds a global name dictionary.

1. **Links.** Link files are split in parallel. The persons map is built in parallel before the parse. Link endpoints are split by key hash into 64 regions of one flat table, and each region gives its IDs dense indexes. Union-find then runs over those indexes, and each region maps its IDs to their person's name, the smallest ID. An unlinked ID names itself.
2. **Parse.** Each worker reads 4 MB chunks of the memory-mapped inputs. Fields are found from 64-byte separator and quote bitmasks (AVX2 or NEON). Timestamps are validated and converted with 8-byte loads and a month table. Every event line with a non-empty `event_id` becomes a 48-byte record holding the event already packed as one integer (time, price, and category for purchases), or all ones if it fails validation, so an invalid earliest copy still wins dedup. Records are scattered into blocks by `event_id` hash; nothing is reallocated or copied.
3. **Dedup.** Each `event_id` bucket is deduplicated in a small open-addressing table, where the earliest `ingest_ts` wins. Valid survivors are read back in memory order, resolved to their person with prefetched lookups, and scattered by person hash together with the valid clicks. A finished bucket's memory takes the next writes.
4. **Sessions.** Each person bucket (about 16k events) is radix-sorted by time at 1 s granularity and grouped stably by person. An insertion pass per person finishes the order. Sessions split on gaps of more than 30 minutes, and belong to their start's UTC day. The credited click is the latest one in the inclusive 7-day window, with ties going to the smallest `click_id`. Assists come straight from each session's click window. For very active persons they come instead from merged per-campaign `[click, click + 7 days]` intervals. Campaign totals go into a dense campaign-by-day array.
5. **Output.** A minimal Parquet writer emits one uncompressed PLAIN page per column: one `user_daily` file per worker, and one `campaign_daily` file. The process exits without freeing anything, and a helper thread unmaps the inputs.

## Reproduce the checks

The track provides the starter's `scripts/` and `fixtures/`, the dataset generator, and the `dev-answer/` reference tables. Keep these downloads outside the source tree. Python with Polars runs the organizers' checkers.

```sh
make && make test   # native build; fuzzes the fast parsing paths against scalar references
PY=python3 STARTER=path/to/starter BENCH=path/to/datasets scripts/check.sh   # fixtures, dev answer
```

`scripts/vm_bench.sh` gives interleaved timings of Linux builds inside the Docker VM, and `scripts/ab_stats.py` summarises them as medians.

## Measured results

The pipeline passes all 36 official fixtures. It matches the official seed-1 answer exactly, both natively and as the amd64 image. On seven more generated seeds, with 1M to 8M events, its output matches an independent pipeline row for row.

On the 20M-event practice dataset, Linux arm64 in Docker on an Apple M5 with 8 CPUs, the median run takes 0.93 to 0.96 s from process start to the last output file. The previous version of this repository took 3.7 s natively on the same data. Peak RSS is about 3.5 GB, half of it the mapped input. Scorer timings on Intel Xeons differ from these.
