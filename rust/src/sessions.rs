// Per-person work: sessions, user-day rows, credited campaign, assists.
use crate::base::*;
use crate::text::*;

pub const GAP: i64 = 30 * MINUTE;
pub const WEEK: i64 = 7 * DAY;
pub const NO_CATEGORY: u32 = u32::MAX;

// A person's events and clicks live in memory as single integers that sort
// in time order. E is u64 when the window, prices and categories fit:
//   event  = rel_ts << 32 | price << 8 | category
//   click  = rel_ts << 32 | position
// and u128 (64-bit halves) otherwise. Rel holds a click time relative to
// the click window start: 32 bits when events pack in u64.
pub trait Ev: Copy + Ord + Send + Sync + 'static {
    type Rel: Copy + Send + Sync;
    const DROPPED: Self; // all ones: an event dedup dropped
    fn event(rel: u64, cat: u32, price: u32) -> Self;
    fn cat(self) -> u32;
    fn price(self) -> u32;
    fn pair(hi: u64, lo: u64) -> Self;
    fn hi(self) -> u64;
    fn lo(self) -> u64;
    fn rel(v: u64) -> Self::Rel;
    fn rel_value(r: Self::Rel) -> u64;
}
impl Ev for u64 {
    type Rel = u32;
    const DROPPED: u64 = !0;
    #[inline(always)]
    fn event(rel: u64, cat: u32, price: u32) -> u64 {
        rel << 32 | (price as u64) << 8 | cat as u64
    }
    #[inline(always)]
    fn cat(self) -> u32 {
        (self & 0xFF) as u32
    }
    #[inline(always)]
    fn price(self) -> u32 {
        (self as u32) >> 8
    }
    #[inline(always)]
    fn pair(hi: u64, lo: u64) -> u64 {
        hi << 32 | lo
    }
    #[inline(always)]
    fn hi(self) -> u64 {
        self >> 32
    }
    #[inline(always)]
    fn lo(self) -> u64 {
        self & 0xFFFFFFFF
    }
    #[inline(always)]
    fn rel(v: u64) -> u32 {
        v as u32
    }
    #[inline(always)]
    fn rel_value(r: u32) -> u64 {
        r as u64
    }
}
impl Ev for u128 {
    type Rel = u64;
    const DROPPED: u128 = !0;
    #[inline(always)]
    fn event(rel: u64, cat: u32, price: u32) -> u128 {
        (rel as u128) << 64 | (price as u128) << 32 | cat as u128
    }
    #[inline(always)]
    fn cat(self) -> u32 {
        self as u32
    }
    #[inline(always)]
    fn price(self) -> u32 {
        ((self as u64) >> 32) as u32
    }
    #[inline(always)]
    fn pair(hi: u64, lo: u64) -> u128 {
        (hi as u128) << 64 | lo as u128
    }
    #[inline(always)]
    fn hi(self) -> u64 {
        (self >> 64) as u64
    }
    #[inline(always)]
    fn lo(self) -> u64 {
        self as u64
    }
    #[inline(always)]
    fn rel(v: u64) -> u64 {
        v
    }
    #[inline(always)]
    fn rel_value(r: u64) -> u64 {
        r
    }
}

// Time is sorted at this granularity (2^COARSE ms) by the bucket radix
// sort; each person's array is then finished by an insertion pass, which is
// nearly free on nearly sorted input.
pub const COARSE: u32 = 10;

// Stable LSD radix sort of the pairs (a[i], b[i]) by a[i].hi() >> COARSE,
// 11 bits a pass; max_hi bounds hi.
pub fn radix_by_time<E: Ev, B: Copy>(
    a: &mut Vec<E>,
    b: &mut Vec<B>,
    ta: &mut Vec<E>,
    tb: &mut Vec<B>,
    max_hi: u64,
    hist: &mut Vec<u32>,
) {
    let max_hi = max_hi >> COARSE;
    let n = a.len();
    if n < 2 || max_hi == 0 {
        return;
    }
    if ta.len() < n {
        ta.resize(n, a[0]);
        tb.resize(n, b[0]);
    }
    ta.truncate(n);
    tb.truncate(n);
    hist.resize(2048, 0);
    let bits = 64 - max_hi.leading_zeros();
    let mut shift = 0;
    while shift < bits {
        hist.fill(0);
        for x in a.iter() {
            hist[((x.hi() >> (shift + COARSE)) & 2047) as usize] += 1;
        }
        let mut sum = 0u32;
        let mut one_digit = false;
        for h in hist.iter_mut() {
            let c = *h;
            one_digit |= c as usize == n;
            *h = sum;
            sum += c;
        }
        if !one_digit {
            for i in 0..n {
                let x = unsafe { *a.get_unchecked(i) };
                let slot = unsafe { hist.get_unchecked_mut(((x.hi() >> (shift + COARSE)) & 2047) as usize) };
                let q = *slot as usize;
                *slot += 1;
                unsafe {
                    *ta.get_unchecked_mut(q) = x;
                    *tb.get_unchecked_mut(q) = *b.get_unchecked(i);
                }
            }
            std::mem::swap(a, ta);
            std::mem::swap(b, tb);
        }
        shift += 11;
    }
}

// Finishes a nearly sorted array.
#[inline(always)]
pub fn insertion_finish<T: Ord + Copy>(a: &mut [T]) {
    for i in 1..a.len() {
        if a[i] >= a[i - 1] {
            continue;
        }
        let x = a[i];
        let mut j = i;
        loop {
            a[j] = a[j - 1];
            j -= 1;
            if j == 0 || x >= a[j - 1] {
                break;
            }
        }
        a[j] = x;
    }
}

#[derive(Clone, Copy, Default)]
pub struct Window {
    pub start: i64, // events [start, end), clicks [click_start, end)
    pub end: i64,
    pub click_start: i64,
    pub day0: i64, // epoch day of start
    pub days: u32,
}

#[derive(Clone, Copy)]
pub struct UserRow {
    pub name: Key,
    pub day: i32,
    pub sessions: u32,
    pub events: u32,
    pub top: u32,
    pub revenue: u64,
}

// (campaign, day) -> sessions, purchasing sessions, revenue, assisted sessions.
pub struct CampaignCells {
    pub days: u32,
    pub cells: Vec<[u64; 4]>,
}
impl CampaignCells {
    #[inline(always)]
    pub fn at(&mut self, campaign: u32, day: u32) -> &mut [u64; 4] {
        &mut self.cells[campaign as usize * self.days as usize + day as usize]
    }
}

// Up to this many sessions x clicks, assists come straight from each
// session's window of clicks; above it, from merged per-campaign intervals.
const DIRECT_ASSISTS: usize = 4096;

#[derive(Clone, Copy)]
struct Session {
    start: u64, // relative to the click window start
    day: u32,
    events: u32,
    lo: u32,
    hi: u32,
    revenue: u64,
}

pub struct PersonWork<E: Ev> {
    w: Window,
    organic: u32,
    sessions: Vec<Session>,
    spend: Vec<u64>,
    touched: Vec<u32>,
    stamp: Vec<u64>,
    stamp_id: u64,
    pairs: Vec<E>,
    starts: Vec<u64>,
    day_end: Vec<u32>,
    pub rows: Vec<UserRow>,
    pub cells: CampaignCells,
}

impl<E: Ev> PersonWork<E> {
    pub fn new(w: Window, campaigns: u32, categories: u32) -> Self {
        PersonWork {
            w,
            organic: campaigns,
            sessions: Vec::new(),
            spend: vec![0; categories as usize],
            touched: Vec::new(),
            stamp: vec![0; campaigns as usize + 1],
            stamp_id: 0,
            pairs: Vec::new(),
            starts: Vec::new(),
            day_end: Vec::new(),
            rows: Vec::new(),
            cells: CampaignCells { days: w.days, cells: vec![[0; 4]; (campaigns as usize + 1) * w.days as usize] },
        }
    }

    #[inline(always)]
    fn credit(cells: &mut CampaignCells, campaign: u32, s: &Session) {
        let c = cells.at(campaign, s.day);
        c[0] += 1;
        c[1] += (s.revenue != 0) as u64;
        c[2] += s.revenue;
    }

    // ev: the person's events, in time order at COARSE granularity; ck:
    // click keys pair(ts - click_start, pos), likewise, with cid[pos] and
    // camp[pos] the rest of each click. Both are finished here.
    pub fn run(&mut self, name: Key, ev: &mut [E], ck: &mut [E], cid: &[Key], camp: &[u32]) {
        insertion_finish(ev);
        insertion_finish(ck);
        let w = self.w;
        let n = ev.len();
        let m = ck.len();
        self.sessions.clear();
        let offset = (w.start - w.click_start) as u64;
        let mut i = 0;
        while i < n {
            let lo = i;
            let first = ev[i].hi();
            let mut prev = first;
            let mut revenue = 0u64;
            while i < n && ev[i].hi() - prev <= GAP as u64 {
                prev = ev[i].hi();
                revenue += ev[i].price() as u64;
                i += 1;
            }
            let start = w.start + first as i64;
            let day = (floor_div(start, DAY) - w.day0) as u32;
            self.sessions.push(Session {
                start: first + offset,
                day,
                events: (i - lo) as u32,
                lo: lo as u32,
                hi: i as u32,
                revenue,
            });
        }
        let ns = self.sessions.len();

        // user_daily
        let mut k = 0;
        while k < ns {
            let day = self.sessions[k].day;
            let mut row =
                UserRow { name, day: (w.day0 + day as i64) as i32, sessions: 0, events: 0, top: NO_CATEGORY, revenue: 0 };
            while k < ns && self.sessions[k].day == day {
                let s = self.sessions[k];
                k += 1;
                row.sessions += 1;
                row.events += s.events;
                if s.revenue == 0 {
                    continue;
                }
                row.revenue += s.revenue;
                for e in &ev[s.lo as usize..s.hi as usize] {
                    let price = e.price();
                    if price == 0 {
                        continue;
                    }
                    let c = e.cat() as usize;
                    if self.spend[c] == 0 {
                        self.touched.push(c as u32);
                    }
                    self.spend[c] += price as u64;
                }
            }
            if !self.touched.is_empty() {
                let mut best = self.touched[0];
                for &c in &self.touched {
                    let (sc, sb) = (self.spend[c as usize], self.spend[best as usize]);
                    if sc > sb || (sc == sb && c < best) {
                        best = c;
                    }
                }
                row.top = best;
                for &c in &self.touched {
                    self.spend[c as usize] = 0;
                }
                self.touched.clear();
            }
            self.rows.push(row);
        }

        // credit
        if m == 0 {
            for s in &self.sessions {
                Self::credit(&mut self.cells, self.organic, s);
            }
            return;
        }
        let direct = ns * m <= DIRECT_ASSISTS;
        let (mut j, mut run, mut chosen_run, mut lo) = (0usize, 0usize, usize::MAX, 0usize);
        let mut chosen = self.organic;
        for si in 0..ns {
            let s = self.sessions[si];
            while j < m && ck[j].hi() <= s.start {
                if j == 0 || ck[j].hi() != ck[j - 1].hi() {
                    run = j;
                }
                j += 1;
            }
            let mut campaign = self.organic;
            if j > 0 && s.start - ck[j - 1].hi() <= WEEK as u64 {
                if chosen_run != run {
                    // the latest timestamp; among equal ones the smallest click_id
                    let mut best = ck[run].lo() as usize;
                    for q in run + 1..j {
                        let pos = ck[q].lo() as usize;
                        if key_less(cid[pos], cid[best]) {
                            best = pos;
                        }
                    }
                    chosen_run = run;
                    chosen = camp[best];
                }
                campaign = chosen;
            }
            Self::credit(&mut self.cells, campaign, &s);
            if direct {
                while lo < j && ck[lo].hi() + (WEEK as u64) < s.start {
                    lo += 1;
                }
                self.stamp_id += 1;
                for q in lo..j {
                    let c = camp[ck[q].lo() as usize];
                    if self.stamp[c as usize] != self.stamp_id {
                        self.stamp[c as usize] = self.stamp_id;
                        self.cells.at(c, s.day)[3] += 1;
                    }
                }
            }
        }
        if direct {
            return;
        }

        // A campaign assists exactly the sessions starting inside the union of
        // [click.ts, click.ts + 7 days] over its clicks.
        self.starts.resize(ns, 0);
        self.day_end.resize(ns, 0);
        for k in (0..ns).rev() {
            self.starts[k] = self.sessions[k].start;
            self.day_end[k] = if k + 1 < ns && self.sessions[k + 1].day == self.sessions[k].day {
                self.day_end[k + 1]
            } else {
                (k + 1) as u32
            };
        }
        self.pairs.clear();
        for q in 0..m {
            self.pairs.push(E::pair(camp[ck[q].lo() as usize] as u64, ck[q].hi()));
        }
        self.pairs.sort_unstable();
        let mut q = 0;
        while q < m {
            let c = self.pairs[q].hi() as u32;
            let mut a = self.pairs[q].lo();
            let mut b = a + WEEK as u64;
            q += 1;
            loop {
                let same = q < m && self.pairs[q].hi() as u32 == c;
                if same && self.pairs[q].lo() <= b {
                    b = self.pairs[q].lo() + WEEK as u64;
                    q += 1;
                    continue;
                }
                let mut x = self.starts.partition_point(|&v| v < a);
                let y = self.starts.partition_point(|&v| v <= b);
                while x < y {
                    let e = (self.day_end[x] as usize).min(y);
                    self.cells.at(c, self.sessions[x].day)[3] += (e - x) as u64;
                    x = e;
                }
                if !same {
                    break;
                }
                a = self.pairs[q].lo();
                b = a + WEEK as u64;
                q += 1;
            }
        }
    }
}
