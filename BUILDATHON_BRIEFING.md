# Builders Buildathon Track Briefing

Submissions close **Sunday, November 22, 2026 at 11:00 a.m. EST**. Each track pays **$5,000 to its winner**. The practice board determines who qualifies in Mech Royale, while Shopping Sessions and Blast Radius rerun entries on fresh inputs. CAD De-ID awards the prize based on the board at cutoff. [Event rules](https://www.joinbuilders.org/buildathon#rules) · [Track pages](https://www.joinbuilders.org/buildathon#problems)

## Eligibility and submissions

- Current students can compete remotely, solo or in teams of two. Each person can join one team, and each team can enter multiple tracks. AI is allowed subject to track-specific rules. Work built during the event must use the MIT License, and repo-based entries should identify pre-existing code in the README. [Event rules](https://www.joinbuilders.org/buildathon#rules)
- Each track allows one outstanding submission per team, including entries waiting in the queue. Rejections consume an hourly slot and leave the previous best score in place. For tracks with final reruns, you can select an entry before the cutoff. The best practice entry is the default. [Submission guide](https://www.joinbuilders.org/buildathon#start)
- Mech Royale, Shopping Sessions, and Blast Radius take a public GitHub repository with a root Dockerfile. The repository limit is 20 MB for Mech Royale and Blast Radius, and 50 MB for Shopping Sessions. Keep submitted commits accessible through results publication. CAD De-ID takes an uploaded attempt folder with the processed corpus, pipeline source, and reproducibility instructions. Keep the raw CAD input and private identifier mappings out of the submission. [Mech Royale](https://www.joinbuilders.org/buildathon/tracks/builders) · [Shopping Sessions](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions) · [Blast Radius](https://www.joinbuilders.org/buildathon/tracks/blast-radius) · [CAD De-ID](https://www.joinbuilders.org/buildathon/tracks/machina-deid)

## Submission caps and leaderboard

Leaderboard snapshot from **around 12:44 a.m. EST on Saturday, November 21**. Team counts and rankings can change during the event. [Live leaderboard](https://www.joinbuilders.org/buildathon/leaderboard) · [Posted submission rates](https://www.joinbuilders.org/buildathon/status)

| Track | Per-team cap | Teams listed | Top three at the snapshot |
| --- | --- | ---: | --- |
| Builders Mech Royale | 4 per hour | 163 | A-bot LLC 5.58, scorpion 5.58, AT 5.52 points |
| Databricks Shopping Sessions | 2 per hour | 158 | Mahalas 0.65 s, Unlimited PTO 0.66 s, Sher-E-Umiam 0.66 s |
| Machina CAD De-ID | 2 per hour, 20 scored attempts total | 30 | 67 with 744, ND with 545, Monsters with 540 checks passed |
| RWX Blast Radius | 1 per hour at the posted rate | 149 | fortnite pirate token 149.7 s, 67 at 151.8 s, SophiaOmri 153.7 s |

At 12:23 a.m. EST that morning, the status page showed all systems operational. Shopping Sessions had an empty queue and 112 scored runs in the previous hour. [Scoring status](https://www.joinbuilders.org/buildathon/status)

## Builders Mech Royale

Program a bot for a 16-mech battle royale on a virtual Ohio State campus. The score is average `16 − finishing place`, so every position gained is worth a point. Practice uses 48 matches against 15 cooperating house bots. The top 32 teams qualify and 16 advance to a final free-for-all series on new seeds. The bot has **50 ms** to answer each observation. [Track rules](https://www.joinbuilders.org/buildathon/tracks/builders)

**Strategy.** Start with reliable navigation, early movement into the next safe zone, and a simple policy for pursuing loot or disengaging. Track how often the bot gets stuck, misses a decision deadline, or takes zone damage. Use the four hourly slots to compare individual behavior changes. Test across varied spawns and account for the shift from coordinated house bots to opponents competing against each other. [Bot protocol](https://www.joinbuilders.org/buildathon/tracks/builders/files/protocol.md?inline=1)

## Databricks Shopping Sessions

Clean clickstream logs, link shopper IDs, build sessions, and attribute activity to ad campaigns. The required outputs are exact daily Parquet tables for shoppers and campaigns. The scorer checks the public 1M-event dataset first, then times the pipeline on about 20M hidden practice events. The final takes the median of three runs on about 60M fresh events. Any run over three minutes eliminates the final score. At least the 10 fastest entries get seven isolated retests. [Track rules](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions)

**Strategy.** Pass the 36 edge-case fixtures and the 1M-event check before optimizing. Test transitive IDs, deduplication before validation, and exact attribution-window boundaries. Profile parsing, sorting, and the seven-day campaign window on several generated seeds. Measure peak memory and full container runtime at larger scale, especially with unusually active shoppers. Choose a final commit with repeatable runtime and enough headroom under the cap. [Exact specification](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions/files/rules.md?inline=1)

## Machina CAD De-ID

Replace identifying information with consistent pseudonyms across **139.7 GB** of native CAD files, drawings, PDFs, and archives. Preserve usable files, geometry, and references between drawings, models, and assemblies. The scorer reports totals by category across 1,000 private checks. Rejections consume an hourly slot and do not use one of the 20 scored attempts. The highest scored attempt at cutoff stands. [Track rules](https://www.joinbuilders.org/buildathon/tracks/machina-deid)

**Strategy.** Run the full download, processing, and upload workflow on a representative sample early. Use one mapping across filenames, metadata, document text, and archive contents. Update references alongside renames and verify that assemblies still open. Batch related fixes so category-level feedback can guide the next attempt. Leave time for the full upload and grading before cutoff. [Submission and grading guide](https://www.joinbuilders.org/buildathon/tracks/machina-deid)

## RWX Blast Radius

For each patch to the pinned `sqlglot` commit, select tests to run and distribute them across up to eight workers. A patch with selected tests costs the busiest worker’s test time plus **two seconds per worker**. A patch with any missed failing test adds **300 seconds**, and the selector’s CPU time counts toward the batch score. The final uses unseen patches. The rules allow coverage, import, and fixture maps made from the pinned commit and prohibit precomputed patch-to-answer lookups. [Track rules](https://www.joinbuilders.org/buildathon/tracks/blast-radius) · [Scoring specification](https://www.joinbuilders.org/buildathon/tracks/blast-radius/files/SCORING.md?inline=1)

**Strategy.** Use dependency or coverage maps to identify affected tests, with broader selection for common infrastructure or uncertain changes. Use the 30-patch local sample to check for misses before spending the hourly slot. Once selection is reliable, balance tests by recorded duration and use another worker when the reduction in the busiest worker’s load exceeds the two-second overhead. [Starter and local scorer](https://www.joinbuilders.org/buildathon/tracks/blast-radius)
