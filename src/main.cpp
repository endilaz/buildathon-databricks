#include "aggregate.hpp"
#include "dedup.hpp"
#include "output.hpp"
#include <array>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <map>

namespace fs = std::filesystem;

namespace shopping {

constexpr size_t partitions = 64;
constexpr int64_t day_ms = 86400000;
constexpr int64_t week_ms = 7 * day_ms;

struct Catalog {
    MappedFile products_file, campaigns_file;
    std::vector<Text> categories, campaigns, channels;
    std::unordered_map<Text, Index> products, campaign_index;
    Index unknown, organic;
    explicit Catalog(const fs::path& dir)
        : products_file(dir / "products.csv"), campaigns_file(dir / "campaigns.csv") {
        std::map<Text, Index> ranks;
        ranks.emplace("unknown", 0);
        std::vector<std::pair<Text, Text>> pairs;
        CsvReader product_rows(csv_data(products_file.view()));
        CsvFields<2> f;
        Text line;
        while (product_rows.next(line)) {
            if (f.parse(line)) {
                ranks.emplace(f[1], 0);
                pairs.emplace_back(f[0], f[1]);
            }
        }
        for (auto& [name, rank] : ranks) {
            rank = as_index(categories.size());
            categories.push_back(name);
        }
        unknown = ranks.at("unknown");
        products.reserve(pairs.size());
        for (auto [product, category] : pairs) products.emplace(product, ranks.at(category));
        CsvReader campaign_rows(csv_data(campaigns_file.view()));
        while (campaign_rows.next(line)) {
            if (!f.parse(line)) continue;
            campaign_index.emplace(f[0], as_index(campaigns.size()));
            campaigns.push_back(f[0]);
            channels.push_back(f[1]);
        }
        organic = as_index(campaigns.size());
        campaigns.emplace_back("organic");
        channels.emplace_back("organic");
    }
};

struct Candidate { Text id, line; int64_t ingest; };
struct Records { std::vector<Event> events; std::vector<Click> clicks; };
struct Worker : Records {
    LocalPeople people;
    std::array<std::vector<Candidate>, partitions> repeated;
};

class Job {
    Window window_;
    Files links_, events_, clicks_;
    People linked_;
    std::vector<Text> names_;
    Index first_single_ = 0;
    Catalog catalog_;
    size_t threads_;
    std::vector<Worker> workers_;
    std::array<Records, partitions> groups_;
    bool profile_;
    std::chrono::steady_clock::time_point mark_ = std::chrono::steady_clock::now();

    void mark(const char* stage) {
        auto now = std::chrono::steady_clock::now();
        if (profile_) std::cerr << stage << " " << std::chrono::duration<double>(now - mark_).count() << " s\n";
        mark_ = now;
    }
    void link_people() {
        for (auto chunk : links_.chunks) {
            CsvReader rows(chunk);
            CsvFields<2> f;
            Text line;
            while (rows.next(line)) {
                if (f.parse(line) && !f[0].empty() && !f[1].empty()) linked_.link(f[0], f[1]);
            }
        }
        names_ = linked_.finalize();
        first_single_ = as_index(names_.size());
    }
    void keep(Worker& worker, const CsvFields<7>& f, int64_t ingest) {
        int64_t ts;
        if (f[1].empty() || f[5].empty() || !parse_timestamp(f[2], ts) ||
            ts < window_.start || ts >= window_.end || ingest - ts > day_ms) return;
        int64_t price = 0;
        Index category = catalog_.unknown;
        if (f[4] == "purchase") {
            if (!parse_i64(f[6], price) || price < 1 || price > 10000000) return;
            auto it = catalog_.products.find(f[5]);
            if (it != catalog_.products.end()) category = it->second;
        } else if (f[4] != "view" && f[4] != "cart") {
            return;
        }
        Event event{};
        event.person = worker.people.get(f[1], linked_, first_single_);
        event.ts = ts;
        event.price = static_cast<int32_t>(price);
        event.category = category;
        worker.events.push_back(event);
    }
    void read_events(const DuplicateFilter& duplicates) {
        parallel(threads_, events_.chunks.size(), [&](size_t tid, size_t i) {
            auto& worker = workers_[tid];
            CsvReader rows(events_.chunks[i]);
            CsvFields<7> f;
            Text line;
            while (rows.next(line)) {
                if (!f.parse(line) || f[0].empty()) continue;
                int64_t ingest;
                if (!parse_timestamp(f[3], ingest)) continue; // Guaranteed valid F1 by the input contract.
                if (duplicates.maybe_repeated(f[0])) {
                    auto shard = std::hash<Text>{}(f[0]) % partitions;
                    worker.repeated[shard].push_back({f[0], line, ingest});
                } else {
                    keep(worker, f, ingest);
                }
            }
        });
    }
    void resolve_duplicates() {
        parallel(threads_, partitions, [&](size_t tid, size_t shard) {
            size_t count = 0;
            for (auto& w : workers_) count += w.repeated[shard].size();
            std::unordered_map<Text, const Candidate*> winners;
            winners.reserve(count);
            for (auto& w : workers_) {
                for (auto& candidate : w.repeated[shard]) {
                    auto [it, fresh] = winners.try_emplace(candidate.id, &candidate);
                    if (!fresh && candidate.ingest < it->second->ingest) it->second = &candidate;
                }
            }
            CsvFields<7> f;
            for (auto [id, chosen] : winners) {
                f.parse(chosen->line);
                keep(workers_[tid], f, chosen->ingest);
            }
            for (auto& w : workers_) std::vector<Candidate>().swap(w.repeated[shard]);
        });
    }
    void read_clicks() {
        parallel(threads_, clicks_.chunks.size(), [&](size_t tid, size_t i) {
            auto& worker = workers_[tid];
            CsvReader rows(clicks_.chunks[i]);
            CsvFields<4> f;
            Text line;
            while (rows.next(line)) {
                if (!f.parse(line) || f[0].empty() || f[1].empty()) continue;
                int64_t ts;
                if (!parse_timestamp(f[3], ts) || ts < window_.start - week_ms || ts >= window_.end) continue;
                auto campaign = catalog_.campaign_index.find(f[2]);
                if (campaign == catalog_.campaign_index.end()) continue;
                Click click{};
                click.person = worker.people.get(f[1], linked_, first_single_);
                click.ts = ts;
                click.campaign = campaign->second;
                click.id = f[0];
                worker.clicks.push_back(click);
            }
        });
    }
    template<class T>
    void distribute(std::vector<T> Records::* field) {
        using Counts = std::array<size_t, partitions>;
        std::vector<Counts> offsets(workers_.size());
        parallel(threads_, workers_.size(), [&](size_t, size_t i) {
            for (auto& row : workers_[i].*field) {
                row.person = workers_[i].people.global(row.person, first_single_);
                ++offsets[i][row.person % partitions];
            }
        });
        for (size_t p = 0; p < partitions; ++p) {
            size_t total = 0;
            for (auto& counts : offsets) {
                auto n = counts[p];
                counts[p] = total;
                total += n;
            }
            (groups_[p].*field).resize(total);
        }
        parallel(threads_, workers_.size(), [&](size_t, size_t i) {
            for (auto& row : workers_[i].*field) {
                auto p = row.person % partitions;
                (groups_[p].*field)[offsets[i][p]++] = row;
            }
            std::vector<T>().swap(workers_[i].*field);
        });
    }
    void partition() {
        NameIndex global;
        for (auto& w : workers_) {
            w.people.remap.reserve(w.people.names.size());
            for (auto name : w.people.names) {
                Index next = as_index(names_.size());
                Index id = global.get(name, next);
                if (id == next) names_.push_back(name);
                w.people.remap.push_back(id);
            }
        }
        distribute(&Records::events);
        distribute(&Records::clicks);
        std::vector<Worker>().swap(workers_);
    }
    void aggregate_and_write(const fs::path& out);
public:
    Job(const fs::path& data, size_t threads, bool profile)
        : window_(read_manifest(data / "manifest.json")), links_(data / "links"), events_(data / "events"),
          clicks_(data / "clicks"), catalog_(data), threads_(threads), workers_(threads), profile_(profile) {}
    void run(const fs::path& output) {
        link_people(); mark("links");
        {
            DuplicateFilter duplicates(events_, threads_); mark("duplicate filter");
            read_events(duplicates); mark("event parse");
        }
        resolve_duplicates(); mark("duplicate resolution");
        read_clicks(); mark("click parse");
        partition(); mark("person partition");
        aggregate_and_write(output); mark("parquet output");
    }
};

} // namespace shopping

namespace shopping {

void Job::aggregate_and_write(const fs::path& out) {
    fs::create_directories(out / "user_daily");
    fs::create_directories(out / "campaign_daily");
    std::array<AggregateResult, partitions> results;
    parallel(threads_, partitions, [&](size_t, size_t i) {
        results[i] = aggregate(groups_[i].events, groups_[i].clicks, catalog_.categories.size(), catalog_.organic);
        groups_[i] = Records{};
    });
    mark("sessionization and attribution");
    bool any_users = false;
    for (auto& result : results) any_users |= !result.users.empty();
    parallel(threads_, partitions, [&](size_t, size_t i) {
        auto& users = results[i].users;
        auto path = out / "user_daily" / ("part-" + std::to_string(i) + ".parquet");
        if (users.empty() && (i != 0 || any_users)) {
            fs::remove(path); // A previous run can have had data in this partition.
            return;
        }
        output::write(path, true, users.size(), [&](output::Batch& b, size_t offset) {
            auto row = [&](size_t j) -> const auto& { return users[offset+j]; };
            b.strings([&](auto j) { return names_[row(j).person]; });
            b.values<parquet::Int32Type>([&](auto j) { return row(j).day; });
            b.values<parquet::Int64Type>([&](auto j) { return row(j).sessions; });
            b.values<parquet::Int64Type>([&](auto j) { return row(j).events; });
            b.values<parquet::Int64Type>([&](auto j) { return row(j).revenue; });
            b.nullable_strings([&](auto j) -> std::optional<Text> {
                if (row(j).revenue == 0) return {};
                return catalog_.categories[row(j).top_category];
            });
        });
    });
    std::map<std::pair<Index, int32_t>, CampaignRow> merged;
    for (auto& result : results) {
        for (auto& row : result.campaigns) {
            auto& sum = merged[{row.campaign, row.day}];
            sum.campaign = row.campaign;
            sum.day = row.day;
            sum.sessions += row.sessions;
            sum.purchasing_sessions += row.purchasing_sessions;
            sum.revenue += row.revenue;
            sum.assisted_sessions += row.assisted_sessions;
        }
    }
    std::vector<CampaignRow> campaigns;
    campaigns.reserve(merged.size());
    for (auto& entry : merged) campaigns.push_back(entry.second);
    output::write(out / "campaign_daily" / "part-0.parquet", false, campaigns.size(),
        [&](output::Batch& b, size_t offset) {
            auto row = [&](size_t j) -> const auto& { return campaigns[offset+j]; };
            b.strings([&](auto j) { return catalog_.campaigns[row(j).campaign]; });
            b.strings([&](auto j) { return catalog_.channels[row(j).campaign]; });
            b.values<parquet::Int32Type>([&](auto j) { return row(j).day; });
            b.values<parquet::Int64Type>([&](auto j) { return row(j).sessions; });
            b.values<parquet::Int64Type>([&](auto j) { return row(j).purchasing_sessions; });
            b.values<parquet::Int64Type>([&](auto j) { return row(j).revenue; });
            b.values<parquet::Int64Type>([&](auto j) { return row(j).assisted_sessions; });
        });
}

} // namespace shopping

int main() {
    try {
        const auto* data = std::getenv("DATA_DIR");
        const auto* output = std::getenv("OUTPUT_DIR");
        const auto* threads = std::getenv("NCPU");
        const int count = threads ? std::stoi(threads) : 8;
        shopping::Job job(data ? data : "/data", std::clamp(count, 1, 8), std::getenv("SHOPPING_PROFILE") != nullptr);
        job.run(output ? output : "/output");
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "shopping-sessions: " << e.what() << '\n';
        return 1;
    }
}
