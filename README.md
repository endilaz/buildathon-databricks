# Databricks Shopping Sessions

This C++17 pipeline turns raw clickstream CSVs into the two exact daily Parquet tables for the Databricks buildathon track. It reads the event window from the manifest and runs up to eight workers. The competition times the container from process start through output completion. [Track overview](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions) · [Data specification](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions/files/rules.md?inline=1)

## Run the scored entry point

Build the image at the repository root. Set `DATA` to an absolute path containing a generated dataset. Set `OUT` to a dedicated output directory writable by UID 1000.

```sh
docker build --platform linux/amd64 -t shopping-sessions .
mkdir -p "$OUT"
docker run --rm --platform linux/amd64 --network none --read-only --user 1000 \
  --cpus 8 -e NCPU=8 -v "$DATA:/data:ro" -v "$OUT:/output" shopping-sessions
```

The image writes `user_daily/*.parquet` and `campaign_daily/*.parquet`. The pipeline keeps its working data in memory. Set `SHOPPING_PROFILE=1` to print stage timings to stderr.

## Implementation

1. Read all identity links and join their connected components with union-find. Keep the bytewise smallest original identifier in each component, including link-only IDs. Short identifiers can use an exact packed key. Arbitrary identifiers use a full-string dictionary.
2. Memory-map the CSV files and distribute newline-aligned chunks across workers. A compact bit filter finds event IDs that may repeat. Every candidate goes through exact ID and ingest-time comparisons. An invalid earliest copy suppresses later copies. A collision in the filter only adds work.
3. Assign surviving events and eligible ad clicks to 64 partitions by resolved person. Sort each partition by person and time. A gap strictly greater than 30 minutes starts a session. The UTC start day owns the whole session.
4. For each shopper, move two pointers across the inclusive seven-day click window. The winning click is the latest, with ties settled by the smallest click ID. Per-campaign counts track which campaigns assisted. Identical duplicate clicks leave both the winner and the set of assisting campaigns unchanged.
5. Write each partition's user rows directly to Parquet and merge campaign totals across partitions. The pipeline writes each file under a temporary name and renames it after the Parquet footer closes.

DuckDB SQL and Polars are both viable ways to express the transformations. The native implementation gives direct control over the CSV dialect, global deduplication and the cost of attribution for high-activity shoppers. Its external runtime dependencies are Apache Arrow and Parquet C++ 18.1.0, supplied by the PyArrow wheel. The pipeline code was written for this event. The official generator, fixtures and expected answers are separate test inputs.

## Reproduce the checks

The track provides the starter's `scripts/` and `fixtures/`, the dataset generator, and the `dev-answer/` reference tables. Keep these downloads outside the source tree. The native build uses the C++ headers and libraries in the pinned PyArrow wheel. A Python 3.10+ environment with Polars runs the organizers' checkers.

```sh
python3 -m pip install pyarrow==18.1.0 polars
make
python3 "$STARTER/scripts/run_fixtures.py" "$STARTER/fixtures" -- "$PWD/shopping-sessions"

generate --seed 1 --events 1000000 --clicks 250000 --output "$DATA"
DATA_DIR="$DATA" OUTPUT_DIR="$OUT" NCPU=8 ./shopping-sessions
python3 "$STARTER/scripts/compare.py" "$OUT" "$DEV_ANSWER"
```

For a practice-sized timing, generate a new dataset with 20,000,000 events and 5,000,000 clicks in a fresh directory. Time the process separately from the generator. An Apple Silicon native binary and a Linux amd64 container running under emulation have different timings from the competition's native Intel machines.

## Measured results

On the development Mac, the native executable passed all 36 official fixtures. The official seed-1 comparison matched all 117,038 user-day rows and all 27,400 campaign-day rows, with zero duplicate keys.

With eight workers, three consecutive native runs on seed 21 (20 million events and 5 million clicks) took 4.03, 3.01 and 2.86 seconds. The median was 3.01 seconds and the highest observed process RSS was 3.78 GiB. The CSV input occupied 1.8 GiB and the Parquet output occupied 92 MiB. These machine-local measurements include process startup and writing both output tables. The generator ran before the benchmark.

The same practice dataset took 6.42 seconds inside the Linux amd64 image under Docker Desktop emulation. The cgroup reported a peak of 3.24 GiB with a 6 GiB cap and swap disabled. Its output matched the native run on all 2,219,052 user-day rows and 28,014 campaign-day rows.
