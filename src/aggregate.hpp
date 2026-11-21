#pragma once
#include <algorithm>
#include <cstdint>
#include <limits>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace shopping {

struct Event { int64_t ts; uint32_t person, category; int32_t price; };
struct Click { int64_t ts; uint32_t person, campaign; std::string_view id; };
struct UserRow {
    uint32_t person = 0;
    int32_t day = 0;
    int64_t sessions = 0, events = 0, revenue = 0;
    uint32_t top_category = 0;
};
struct CampaignRow {
    uint32_t campaign = 0;
    int32_t day = 0;
    int64_t sessions = 0, purchasing_sessions = 0, revenue = 0, assisted_sessions = 0;
};
struct AggregateResult {
    std::vector<UserRow> users;
    std::vector<CampaignRow> campaigns;
};

// Each add/remove changes one campaign count. Iterating active_ visits each
// qualifying campaign exactly once even if it supplied thousands of clicks.
class AssistWindow {
    std::vector<uint32_t> counts_, position_;
    std::vector<uint32_t> active_;
public:
    explicit AssistWindow(size_t campaigns) : counts_(campaigns), position_(campaigns) {}
    void add(uint32_t c) {
        if (counts_[c]++ == 0) {
            position_[c] = static_cast<uint32_t>(active_.size());
            active_.push_back(c);
        }
    }
    void remove(uint32_t c) {
        if (--counts_[c] == 0) {
            auto last = active_.back();
            active_[position_[c]] = last;
            position_[last] = position_[c];
            active_.pop_back();
        }
    }
    const std::vector<uint32_t>& active() const { return active_; }
    void clear() {
        for (auto c : active_) counts_[c] = 0;
        active_.clear();
    }
};

inline AggregateResult aggregate(std::vector<Event>& events, std::vector<Click>& clicks,
                                 size_t categories, uint32_t organic) {
    AggregateResult result;
    if (events.empty()) return result;
    std::sort(events.begin(), events.end(), [](const Event& a, const Event& b) {
        return a.person != b.person ? a.person < b.person : a.ts < b.ts;
    });
    // At equal timestamps the lexicographically smallest click is visited last.
    std::sort(clicks.begin(), clicks.end(), [](const Click& a, const Click& b) {
        if (a.person != b.person) return a.person < b.person;
        if (a.ts != b.ts) return a.ts < b.ts;
        return a.id > b.id;
    });
    AssistWindow assists(organic);
    std::vector<int64_t> spend(categories);
    std::vector<uint32_t> touched;
    std::unordered_map<uint64_t, size_t> campaign_indices;
    auto campaign_day = [&](uint32_t c, int32_t day) -> CampaignRow& {
        uint64_t key = (uint64_t(uint32_t(day)) << 32) | c;
        auto [it, fresh] = campaign_indices.try_emplace(key, result.campaigns.size());
        if (fresh) result.campaigns.push_back({c, day});
        return result.campaigns[it->second];
    };
    size_t event = 0, click = 0;
    while (event < events.size()) {
        uint32_t person = events[event].person;
        while (click < clicks.size() && clicks[click].person < person) ++click;
        size_t left = click, right = click;
        UserRow* user = nullptr;
        while (event < events.size() && events[event].person == person) {
            const int64_t start = events[event].ts;
            int32_t day = static_cast<int32_t>(start / 86400000 - (start % 86400000 < 0));
            if (!user || user->day != day) {
                for (auto c : touched) spend[c] = 0;
                touched.clear();
                result.users.push_back({person, day});
                user = &result.users.back();
            }
            size_t first_event = event;
            int64_t revenue = 0, previous;
            do {
                auto& item = events[event++];
                previous = item.ts;
                if (item.price > 0) {
                    if (spend[item.category] == 0) touched.push_back(item.category);
                    spend[item.category] += item.price;
                    if (spend[item.category] > spend[user->top_category] ||
                        (spend[item.category] == spend[user->top_category] && item.category < user->top_category))
                        user->top_category = item.category;
                    revenue += item.price;
                }
            } while (event < events.size() && events[event].person == person && events[event].ts - previous <= 1800000);
            ++user->sessions;
            user->events += event - first_event;
            user->revenue += revenue;

            while (right < clicks.size() && clicks[right].person == person && clicks[right].ts <= start)
                assists.add(clicks[right++].campaign);
            while (left < right && clicks[left].ts < start - 604800000)
                assists.remove(clicks[left++].campaign);
            uint32_t credited = left < right ? clicks[right - 1].campaign : organic;
            auto& row = campaign_day(credited, day);
            ++row.sessions;
            row.purchasing_sessions += revenue != 0;
            row.revenue += revenue;
            for (uint32_t c : assists.active()) ++campaign_day(c, day).assisted_sessions;
        }
        assists.clear();
        click = right;
    }
    return result;
}

} // namespace shopping
