// Shopping Sessions pipeline (a Rust port of the C++ entry in ../src).
//
//   links    parse in parallel; the persons map is built in parallel
//   parse    one pass over events and clicks: each valid event and click is
//            resolved to its person and filed in that person's bucket; every
//            copy of an event also leaves a small record by event_id
//   dedup    per event_id bucket: the earliest ingest wins, and every other
//            copy's filed event is marked dropped where it lies
//   sessions per person bucket: group by person, sessions, credit, assists
//   output   hand-written Parquet, one user_daily file per worker
mod base;
mod memory;
mod parquet;
mod sessions;
mod table;
mod text;

use base::*;
use memory::*;
use sessions::*;
use std::collections::HashMap;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Instant;
use table::*;
use text::*;

#[global_allocator]
static GLOBAL: Alloc = Alloc;

// event_id buckets: about this many lines each (smalldd, half as many so a
// table fits a 1 MB L2, was slightly slower on the x86 bench)
#[cfg(feature = "smalldd")]
const EVENT_BUCKET_LINES: usize = 32768;
#[cfg(not(feature = "smalldd"))]
const EVENT_BUCKET_LINES: usize = 65536;
// person buckets: few enough that staging groups stay in cache
// (SHOPPING_PBE overrides, for sweeps)
const PERSON_BUCKET_EVENTS: usize = 32768;
fn env_size(name: &str, default: usize) -> usize {
    std::env::var(name).ok().and_then(|s| s.parse().ok()).filter(|&v| v > 0).unwrap_or(default)
}

// ---------------------------------------------------------------- records

#[repr(C, packed(8))]
#[derive(Clone, Copy)]
struct PersonEvent<E: Ev> {
    // a valid event, filed under its person; ev is all ones once dedup drops it
    person: Key,
    ev: E,
}
#[repr(C, packed(8))]
#[derive(Clone, Copy)]
struct DedupRec {
    // every parsed copy of an event, by event_id
    eid: Key,
    // where its event was filed (0 when the copy is invalid), and above bit
    // 48 a long eid's check bits: equal keys with unequal checks are a hash
    // collision (base.rs)
    ev_check: u64,
    ingest: i64,
}
impl DedupRec {
    #[inline(always)]
    fn ev<E: Ev>(&self) -> *mut PersonEvent<E> {
        (self.ev_check & PTR_MASK) as *mut PersonEvent<E>
    }
    #[inline(always)]
    fn check(&self) -> u16 {
        (self.ev_check >> 48) as u16
    }
}
#[repr(C, packed(8))]
#[derive(Clone, Copy)]
struct PersonClick<E: Ev> {
    person: Key,
    cid: Key,
    rel: E::Rel,
    camp: u32,
}
const _: () = assert!(
    std::mem::size_of::<DedupRec>() == 24
        && std::mem::size_of::<PersonClick<u64>>() == 24
        && std::mem::size_of::<PersonEvent<u64>>() == 16
        && std::mem::size_of::<PersonEvent<u128>>() == 24
        && std::mem::size_of::<PersonClick<u128>>() == 32
);

// ---------------------------------------------------------------- catalog

struct Catalog {
    categories: Vec<Vec<u8>>, // sorted byte-wise; index = rank
    unknown: u32,
    products: KeyMap<u32>,
    numbered: bool, // products named <prefix><decimal> use by_number
    prefix: u8,
    by_number: Vec<u16>, // 0xFFFF: no such product
    campaigns: KeyMap<u32>,
    campaign_names: Vec<Vec<u8>>,
    channels: Vec<Vec<u8>>,
    _inputs: Vec<Input>, // keeps the mappings the Fields above came from
}

impl Catalog {
    #[inline(always)]
    fn category(&self, pid: Field) -> u32 {
        if self.numbered {
            if let Some(num) = prefixed_number(pid, self.prefix) {
                if num < (1 << 22) {
                    let Some(&c) = self.by_number.get(num as usize) else { return self.unknown };
                    return if c == 0xFFFF { self.unknown } else { c as u32 };
                }
            }
        }
        self.products.find(make_key(pid.bytes())).unwrap_or(self.unknown)
    }

    fn new(dir: &str) -> Catalog {
        let pf = map_input(&format!("{dir}/products.csv"));
        let mut rows: Vec<(Field, Field)> = Vec::new();
        for_lines::<2>(pf.data, pf.len, 0, pf.len, |f| {
            if !f[0].is_empty() {
                rows.push((f[0], f[1]));
            }
        });
        // distinct category names, then ranks in byte-wise order
        let mut rank: HashMap<Vec<u8>, u32> = HashMap::new();
        rank.insert(b"unknown".to_vec(), 0);
        for r in &rows {
            rank.entry(r.1.bytes().to_vec()).or_insert(0);
        }
        let mut categories: Vec<Vec<u8>> = rank.keys().cloned().collect();
        categories.sort();
        for (i, c) in categories.iter().enumerate() {
            *rank.get_mut(c).unwrap() = i as u32;
        }
        let unknown = rank[&b"unknown"[..]];
        let mut first = [0usize; 256];
        for r in &rows {
            first[unsafe { r.0.at(0) } as usize] += 1;
        }
        let mut prefix = 0u8;
        for c in 0..256 {
            if first[c] > first[prefix as usize] {
                prefix = c as u8;
            }
        }
        let numbered = categories.len() < 0xFFFF;
        let mut products = KeyMap::new(rows.len());
        let mut by_number: Vec<u16> = Vec::new();
        for r in &rows {
            let c = rank[r.1.bytes()];
            products.insert(make_key(r.0.bytes()), c);
            if numbered {
                if let Some(num) = prefixed_number(r.0, prefix) {
                    if num < (1 << 22) {
                        if num as usize >= by_number.len() {
                            by_number.resize(num as usize + 1, 0xFFFF);
                        }
                        by_number[num as usize] = c as u16;
                    }
                }
            }
        }

        let cf = map_input(&format!("{dir}/campaigns.csv"));
        let mut campaigns = KeyMap::new(8);
        let mut campaign_names: Vec<Vec<u8>> = Vec::new();
        let mut channels: Vec<Vec<u8>> = Vec::new();
        for_lines::<2>(cf.data, cf.len, 0, cf.len, |f| {
            if f[0].is_empty() {
                return;
            }
            let idx = campaigns.get_or_insert(make_key(f[0].bytes()), || {
                campaign_names.push(f[0].bytes().to_vec());
                channels.push(Vec::new());
                (campaign_names.len() - 1) as u32
            });
            channels[idx as usize] = f[1].bytes().to_vec();
        });
        Catalog {
            categories,
            unknown,
            products,
            numbered,
            prefix,
            by_number,
            campaigns,
            campaign_names,
            channels,
            _inputs: vec![pf, cf],
        }
    }
}

// ---------------------------------------------------------------- links

// Every ID in a kept link line -> its person's name: one flat linear-probing
// table, split by the top bits of the key hash into equal regions that are
// filled in parallel.
const PART_BITS: u32 = 6;
const PARTS: usize = 1 << PART_BITS;

struct Persons {
    slots: Vec<KeySlot<Key>>,
    region_bits: u32, // each region holds 2^region_bits slots
    count: usize,
    // One bit per hash value (about 8 bits per linked ID, so it stays in
    // cache) set for every linked ID: a user whose bit is clear is not
    // linked, and its lookup reads EMPTY_SLOT (always in L1) instead of a
    // slot of the big table. The choice is a select, not a branch.
    filter: Vec<u64>,
    filter_mask: usize, // bits - 1
}
static EMPTY_SLOT: KeySlot<Key> = KeySlot { k: 0, v: 0 };
// filter bits come from the hash above the bits that pick a slot in a region
const FILTER_SHIFT: u32 = 24;

#[inline(always)]
fn part(h: u64) -> usize {
    (h >> (64 - PART_BITS)) as usize
}
#[inline(always)]
fn region_slot(region_bits: u32, h: u64) -> usize {
    (part(h) << region_bits) | (h as usize & ((1usize << region_bits) - 1))
}
#[inline(always)]
fn region_next(region_bits: u32, i: usize) -> usize {
    let mask = (1usize << region_bits) - 1;
    (i & !mask) | ((i + 1) & mask)
}

impl Persons {
    fn empty() -> Persons {
        Persons {
            slots: vec![KeySlot { k: 0, v: 0 }; PARTS],
            region_bits: 0,
            count: 0,
            filter: vec![0; 1],
            filter_mask: 63,
        }
    }
    // At parse time: the slot chosen and prefetched (one stage), or with
    // stage2 only the filter word prefetched and null returned (the slot is
    // chosen LOOKUP_STAGE lines later).
    #[inline(always)]
    fn first_stage(&self, h: u64) -> *const KeySlot<Key> {
        if !cfg!(feature = "stage1") {
            let i = (h >> FILTER_SHIFT) as usize & self.filter_mask;
            prefetch(unsafe { self.filter.as_ptr().add(i >> 6) });
            std::ptr::null()
        } else {
            let slot = self.lookup_slot(h);
            prefetch(slot);
            slot
        }
    }
    // The slot a lookup of user (h = key_hash(user)) should read: its first
    // slot in the table if the filter has its bit, else EMPTY_SLOT.
    #[inline(always)]
    fn lookup_slot(&self, h: u64) -> *const KeySlot<Key> {
        let i = (h >> FILTER_SHIFT) as usize & self.filter_mask;
        let flagged = (unsafe { *self.filter.get_unchecked(i >> 6) } >> (i & 63)) & 1;
        let real = unsafe { self.slots.as_ptr().add(region_slot(self.region_bits, h)) } as usize;
        #[cfg(feature = "nofilter")]
        return {
            let _ = flagged;
            real as *const KeySlot<Key>
        };
        let empty = &EMPTY_SLOT as *const KeySlot<Key> as usize;
        (empty ^ ((real ^ empty) & (flagged as usize).wrapping_neg())) as *const KeySlot<Key>
    }
    // As name_select, reading the slot lookup_slot chose (only a real slot
    // can hold another key, so EMPTY_SLOT never walks).
    #[inline(always)]
    fn name_at(&self, user: Key, h: u64, slot: *const KeySlot<Key>) -> (Key, bool) {
        let s = unsafe { *slot };
        if (s.k != user) & (s.k != 0) {
            return self.name_hashed(user, h);
        }
        let linked = s.k == user;
        (if linked { s.v } else { user }, linked)
    }
    // (the probe's lookups, without the filter)
    #[cfg_attr(not(feature = "probe"), allow(dead_code))]
    // Linked or not is a coin toss from line to line: the first slot's answer
    // is selected, not branched on; only a collision walks on. Also says
    // whether user is linked (so its bytes were recorded with the links).
    #[inline(always)]
    fn name_select(&self, user: Key, h: u64) -> (Key, bool) {
        let s = unsafe { *self.slots.get_unchecked(region_slot(self.region_bits, h)) };
        if (s.k != user) & (s.k != 0) {
            return self.name_hashed(user, h);
        }
        let linked = s.k == user;
        (if linked { s.v } else { user }, linked)
    }
    // h must be key_hash(user)
    fn name_hashed(&self, user: Key, h: u64) -> (Key, bool) {
        let mut i = region_slot(self.region_bits, h);
        loop {
            let s = self.slots[i];
            if s.k == user {
                return (s.v, true);
            }
            if s.k == 0 {
                return (user, false);
            }
            i = region_next(self.region_bits, i);
        }
    }
    #[cfg_attr(not(feature = "probe"), allow(dead_code))]
    #[inline(always)]
    fn prefetch_hashed(&self, h: u64) {
        prefetch(unsafe { self.slots.as_ptr().add(region_slot(self.region_bits, h)) });
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct Endpoint {
    k: Key,
    r: u32, // 2 * pair index + side
}

#[derive(Clone, Copy)]
struct Named {
    key: Key,
    prefix: u64,
}

// Union-find over the link pairs with every step but the unions parallel:
// endpoints are split by key-hash part, each part gives its IDs dense
// indexes in its own region of the final table, the unions run over those
// small integers, and each region then maps its IDs to their person's name.
fn build_persons(pairs: &mut Vec<Vec<(Key, Key)>>, threads: usize) -> Persons {
    let nv = pairs.len();
    let mut base = vec![0usize; nv + 1];
    for t in 0..nv {
        base[t + 1] = base[t] + pairs[t].len();
    }
    let n = base[nv];
    if n == 0 {
        return Persons::empty();
    }
    let mut workers = vec![(); threads];

    let mut spread: Vec<Scatter<Endpoint>> = (0..nv).map(|_| Scatter::new(PARTS)).collect();
    {
        let sp = Shared(spread.as_mut_ptr());
        let pairs = &*pairs;
        let base = &base;
        run_tasks(&mut workers, nv, |_, t| {
            let sc = unsafe { sp.at(t) };
            for (j, &(a, b)) in pairs[t].iter().enumerate() {
                let r = ((base[t] + j) * 2) as u32;
                sc.push(part(key_hash(a)), Endpoint { k: a, r });
                sc.push(part(key_hash(b)), Endpoint { k: b, r: r + 1 });
            }
        });
    }

    // dense index per part; an endpoint's id is (index << PART_BITS) | part
    let most = (0..PARTS).map(|p| spread.iter().map(|sc| sc.size(p)).sum::<usize>()).max().unwrap_or(0);
    // regions sized for every endpoint being distinct, at most 2/3 full
    let mut region_bits = 0u32;
    while (1usize << region_bits) * 2 < most * 3 {
        region_bits += 1;
    }
    // filter: 8 bits per endpoint (SHOPPING_FBITS overrides, for sweeps),
    // at least 2^16, at most 2^30
    let fbits = pow2_at_least(env_size("SHOPPING_FBITS", 8) * 2 * n).clamp(1 << 16, 1 << 30);
    let mut out = Persons {
        slots: vec![KeySlot { k: 0, v: 0 }; PARTS << region_bits],
        region_bits,
        count: 0,
        filter: vec![0; fbits / 64],
        filter_mask: fbits - 1,
    };
    let mut endpoint = vec![0u32; 2 * n];
    let mut sizes = vec![0u32; PARTS];
    {
        let slots = Shared(out.slots.as_mut_ptr());
        let ep = Shared(endpoint.as_mut_ptr());
        let sz = Shared(sizes.as_mut_ptr());
        let spread = &spread;
        run_tasks(&mut workers, PARTS, |_, p| {
            let mut next = 0u32;
            for sc in spread {
                sc.for_each(p, |e| {
                    let (k, r) = (e.k, e.r);
                    // insert into region p (absent: value next)
                    let mut i = region_slot(region_bits, key_hash(k));
                    let v = loop {
                        let s = unsafe { slots.at(i) };
                        if s.k == k {
                            break s.v;
                        }
                        if s.k == 0 {
                            s.k = k;
                            s.v = next as Key;
                            break s.v;
                        }
                        i = region_next(region_bits, i);
                    };
                    if v as u32 == next {
                        next += 1;
                    }
                    unsafe { *ep.at(r as usize) = ((v as u32) << PART_BITS) | p as u32 };
                });
            }
            unsafe { *sz.at(p) = next };
        });
    }
    let mut offset = vec![0u32; PARTS + 1];
    for p in 0..PARTS {
        offset[p + 1] = offset[p] + sizes[p];
    }
    let ids = offset[PARTS];
    out.count = ids as usize;
    let global = |e: u32| offset[(e as usize) & (PARTS - 1)] + (e >> PART_BITS);

    // unions; a non-root's parent always has a smaller index
    let mut parent: Vec<u32> = (0..ids).collect();
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        while parent[x as usize] != x {
            parent[x as usize] = parent[parent[x as usize] as usize];
            x = parent[x as usize];
        }
        x
    }
    for j in 0..n {
        let ra = find(&mut parent, global(endpoint[2 * j]));
        let rb = find(&mut parent, global(endpoint[2 * j + 1]));
        if ra != rb {
            parent[ra.max(rb) as usize] = ra.min(rb);
        }
    }
    // Each ID's first 8 bytes (zero padded, big-endian) order IDs whose
    // prefixes differ, so the sequential pass below rarely needs the bytes
    // of a long ID; they are read here, in parallel.
    let region = 1usize << region_bits;
    let mut name = vec![Named { key: 0, prefix: 0 }; ids as usize];
    {
        let nm = Shared(name.as_mut_ptr());
        let slots = &out.slots;
        let offset = &offset;
        run_tasks(&mut workers, PARTS, |_, p| {
            for s in &slots[p * region..(p + 1) * region] {
                if s.k != 0 {
                    let mut buf = [0u8; 16];
                    let b = key_bytes(s.k, &mut buf);
                    let mut prefix = 0u64;
                    for j in 0..8 {
                        prefix = prefix << 8 | if j < b.len() { b[j] as u64 } else { 0 };
                    }
                    unsafe { *nm.at((offset[p] + s.v as u32) as usize) = Named { key: s.k, prefix } };
                }
            }
        });
    }
    for i in 0..ids as usize {
        let r = parent[parent[i] as usize];
        parent[i] = r;
        let r = r as usize;
        if r == i {
            continue;
        }
        let (a, b) = (name[i], name[r]);
        if if a.prefix != b.prefix { a.prefix < b.prefix } else { key_less(a.key, b.key) } {
            name[r] = a;
        }
    }
    {
        let slots = Shared(out.slots.as_mut_ptr());
        let (name, parent, offset) = (&name, &parent, &offset);
        run_tasks(&mut workers, PARTS, |_, p| {
            for i in p * region..(p + 1) * region {
                let s = unsafe { slots.at(i) };
                if s.k != 0 {
                    s.v = name[parent[(offset[p] + s.v as u32) as usize] as usize].key;
                }
            }
        });
    }
    {
        // set each linked ID's filter bit (words shared between parts: atomic or)
        use std::sync::atomic::AtomicU64;
        let words = unsafe { std::slice::from_raw_parts(out.filter.as_ptr() as *const AtomicU64, out.filter.len()) };
        let (slots, mask) = (&out.slots, out.filter_mask);
        run_tasks(&mut workers, PARTS, |_, p| {
            for s in &slots[p * region..(p + 1) * region] {
                if s.k != 0 {
                    let i = (key_hash(s.k) >> FILTER_SHIFT) as usize & mask;
                    words[i >> 6].fetch_or(1 << (i & 63), Relaxed);
                }
            }
        });
    }
    for v in pairs.iter_mut() {
        *v = Vec::new();
    }
    out
}

// ---------------------------------------------------------------- chunks

struct Chunk<'a> {
    file: &'a Input,
    s: usize,
    e: usize,
}

fn plan_chunks<'a>(files: &'a [Input], target: usize, out: &mut Vec<Chunk<'a>>) {
    for f in files {
        if f.len == 0 {
            continue;
        }
        let parts = f.len.div_ceil(target).max(1);
        let step = f.len.div_ceil(parts);
        let mut s = 0;
        while s < f.len {
            out.push(Chunk { file: f, s, e: f.len.min(s + step) });
            s += step;
        }
    }
}

fn pow2_at_least(n: usize) -> usize {
    n.max(1).next_power_of_two()
}

// ---------------------------------------------------------------- restart

// Runs this program again from the start with var=val in its environment.
fn restart(var: &str, val: &str) -> ! {
    use std::os::unix::process::CommandExt;
    eprintln!("shopping-sessions: restarting with {var}={val}");
    let args: Vec<std::ffi::OsString> = std::env::args_os().collect();
    let exe: std::ffi::OsString =
        if cfg!(target_os = "linux") { "/proc/self/exe".into() } else { args[0].clone() };
    let err = std::process::Command::new(exe).arg0(&args[0]).args(&args[1..]).env(var, val).exec();
    die(&format!("cannot restart: {err}"))
}

// ---------------------------------------------------------------- parse

// Person lookups during the parse run this many lines after their prefetch,
// so each (DRAM) miss overlaps the parsing of the lines between. In two
// stages: a line's filter word is prefetched when it is parsed, and
// LOOKUP_STAGE lines later (the word now cached) its slot is chosen and
// prefetched. The filter word missed L2 on the x86 bench (20% of the event
// path waited on it); feature stage1 chooses the slot at once (faster on
// the M5, whose L2 holds the filter).
// pread pieces (SHOPPING_PIECE overrides, for sweeps)
const READ_PIECE: usize = 256 << 10;
#[cfg(not(feature = "pd16"))]
const PARSE_DELAY: usize = 12;
#[cfg(feature = "pd16")]
const PARSE_DELAY: usize = 16;
const LOOKUP_STAGE: usize = PARSE_DELAY / 2;

// A long ID read at parse time, recorded (base.rs) when its line is flushed,
// by then with its table slot prefetched. The bytes are still in the piece
// being parsed: pieces drain before the next is read.
#[derive(Clone, Copy)]
struct LongRef {
    p: *const u8,
    n: u32,
    check: u16,
}
impl LongRef {
    fn of(f: Field, check: u16) -> LongRef {
        LongRef { p: f.p, n: f.n as u32, check }
    }
    #[inline(always)]
    fn record(&self, k: Key) {
        record_long(k, self.check, unsafe { std::slice::from_raw_parts(self.p, self.n as usize) });
    }
}

#[derive(Clone, Copy)]
struct PendingEvent<E: Ev> {
    eid: Key,
    user: Key,
    ingest: i64,
    ev: E,
    h: u64, // key_hash(user)
    slot: *const KeySlot<Key>, // Persons::lookup_slot(h), prefetched (null until chosen)
    user_ref: LongRef,
    eid_check: u16,
}

struct ParseState<E: Ev> {
    ev: StagedScatter<PersonEvent<E>>, // valid events by person hash
    dd: StagedScatter<DedupRec>,       // every copy by event_id hash
    ck: StagedScatter<PersonClick<E>>, // valid clicks by person hash
    pending_ev: [PendingEvent<E>; PARSE_DELAY],
    pending_ck: [PersonClick<E>; PARSE_DELAY],
    pending_ck_h: [u64; PARSE_DELAY],
    pending_ck_slot: [*const KeySlot<Key>; PARSE_DELAY],
    pending_ck_user: [LongRef; PARSE_DELAY],
    pending_ck_cid: [LongRef; PARSE_DELAY],
    nev: usize,
    nck: usize,
    buf: Vec<u8>, // the chunk being parsed, when inputs are read rather than mapped
}
unsafe impl<E: Ev> Send for ParseState<E> {}
unsafe impl<E: Ev> Sync for ParseState<E> {}

impl<E: Ev> ParseState<E> {
    fn new(event_buckets: usize, person_buckets: usize) -> Self {
        // the pending rings hold plain integers and pointers; zero is a fine start
        unsafe {
            ParseState {
                ev: StagedScatter::new(person_buckets),
                dd: StagedScatter::new(event_buckets),
                ck: StagedScatter::new(person_buckets),
                pending_ev: std::mem::zeroed(),
                pending_ck: std::mem::zeroed(),
                pending_ck_h: [0; PARSE_DELAY],
                pending_ck_slot: [std::ptr::null(); PARSE_DELAY],
                pending_ck_user: std::mem::zeroed(),
                pending_ck_cid: std::mem::zeroed(),
                nev: 0,
                nck: 0,
                buf: Vec::new(),
            }
        }
    }
}

struct Ctx<'a> {
    w: Window,
    catalog: &'a Catalog,
    persons: &'a Persons,
    event_shift: u32,
    person_shift: u32,
}

impl Ctx<'_> {
    #[inline(always)]
    fn flush_event<E: Ev>(&self, st: &mut ParseState<E>, i: usize) {
        let x = st.pending_ev[i];
        // lines drained before their second stage choose the slot now
        let slot = if x.slot.is_null() { self.persons.lookup_slot(x.h) } else { x.slot };
        let (person, linked) = self.persons.name_at(x.user, x.h, slot);
        // a linked ID's bytes were recorded with the links
        #[cfg(feature = "probe")]
        if probe::skip('e') || probe::skip('d') || probe::skip('r') {
            if is_long(x.user) && !linked && !probe::skip('r') {
                x.user_ref.record(x.user);
            }
            let p = if probe::skip('e') {
                std::ptr::null_mut()
            } else {
                st.ev.push((key_hash(person) >> self.person_shift) as usize, PersonEvent { person, ev: x.ev })
            };
            if !probe::skip('d') {
                st.dd.push(
                    (key_hash(x.eid) >> self.event_shift) as usize,
                    DedupRec { eid: x.eid, ev_check: p as u64 | (x.eid_check as u64) << 48, ingest: x.ingest },
                );
            }
            return;
        }
        if is_long(x.user) && !linked {
            x.user_ref.record(x.user);
        }
        let p = st.ev.push((key_hash(person) >> self.person_shift) as usize, PersonEvent { person, ev: x.ev });
        st.dd.push(
            (key_hash(x.eid) >> self.event_shift) as usize,
            DedupRec { eid: x.eid, ev_check: p as u64 | (x.eid_check as u64) << 48, ingest: x.ingest },
        );
    }
    #[inline(always)]
    fn flush_click<E: Ev>(&self, st: &mut ParseState<E>, i: usize) {
        let mut c = st.pending_ck[i];
        let (user, cid) = (c.person, c.cid);
        let h = st.pending_ck_h[i];
        let slot = st.pending_ck_slot[i];
        let slot = if slot.is_null() { self.persons.lookup_slot(h) } else { slot };
        let (person, linked) = self.persons.name_at(user, h, slot);
        c.person = person;
        if is_long(user) && !linked {
            st.pending_ck_user[i].record(user);
        }
        if is_long(cid) {
            st.pending_ck_cid[i].record(cid);
        }
        #[cfg(feature = "probe")]
        if probe::skip('c') {
            return;
        }
        st.ck.push((key_hash(person) >> self.person_shift) as usize, c);
    }
    fn drain<E: Ev>(&self, st: &mut ParseState<E>) {
        for i in st.nev.saturating_sub(PARSE_DELAY)..st.nev {
            self.flush_event(st, i % PARSE_DELAY);
        }
        for i in st.nck.saturating_sub(PARSE_DELAY)..st.nck {
            self.flush_click(st, i % PARSE_DELAY);
        }
        st.nev = 0;
        st.nck = 0;
    }

    #[inline(always)]
    fn parse_event<E: Ev>(&self, f: &[Field; 7], st: &mut ParseState<E>) {
        if f[0].is_empty() {
            return;
        }
        let Some(ingest) = parse_f1(f[3]) else { return };
        let mut eid_check = 0u16;
        let eid = make_key_check(f[0].bytes(), &mut eid_check);
        let w = &self.w;
        if !f[1].is_empty() && !f[5].is_empty() {
            if let Some(ts) = parse_ts(f[2]) {
                if ts >= w.start && ts < w.end && ingest - ts <= DAY {
                    // event_type by fixed-width loads (fields are followed by padding)
                    let ty = f[4];
                    let mut ok = false;
                    let mut price = 0u32;
                    if ty.len() == 8 {
                        if unsafe { load64(ty.p) } == u64::from_le_bytes(*b"purchase") {
                            if let Some(p) = parse_price(f[6]) {
                                price = p;
                                ok = true;
                            }
                        }
                    } else if ty.len() == 4 {
                        let t = unsafe { load32(ty.p) };
                        ok = t == u32::from_le_bytes(*b"view") || t == u32::from_le_bytes(*b"cart");
                    }
                    if ok {
                        // only purchases need a category: top_category sums purchase prices
                        let cat = if price != 0 { self.catalog.category(f[5]) } else { 0 };
                        let mut check = 0u16;
                        let user = make_key_check(f[1].bytes(), &mut check);
                        let h = key_hash(user);
                        let first = self.persons.first_stage(h);
                        if is_long(user) {
                            prefetch_long(user);
                        }
                        let i = st.nev % PARSE_DELAY;
                        if st.nev >= PARSE_DELAY {
                            self.flush_event(st, i);
                        }
                        if !cfg!(feature = "stage1") && st.nev >= LOOKUP_STAGE {
                            let j = (st.nev - LOOKUP_STAGE) % PARSE_DELAY;
                            let slot = self.persons.lookup_slot(st.pending_ev[j].h);
                            prefetch(slot);
                            st.pending_ev[j].slot = slot;
                        }
                        st.pending_ev[i] = PendingEvent {
                            eid,
                            user,
                            ingest,
                            ev: E::event((ts - w.start) as u64, cat, price),
                            h,
                            slot: first,
                            user_ref: LongRef::of(f[1], check),
                            eid_check,
                        };
                        st.nev += 1;
                        return;
                    }
                }
            }
        }
        // an invalid copy files no event, but can still be the earliest copy
        #[cfg(feature = "probe")]
        if probe::skip('d') {
            return;
        }
        st.dd.push(
            (key_hash(eid) >> self.event_shift) as usize,
            DedupRec { eid, ev_check: (eid_check as u64) << 48, ingest },
        );
    }

    #[inline(always)]
    fn parse_click<E: Ev>(&self, f: &[Field; 4], st: &mut ParseState<E>) {
        if f[0].is_empty() || f[1].is_empty() || f[2].is_empty() {
            return;
        }
        let Some(camp) = self.catalog.campaigns.find(make_key(f[2].bytes())) else { return };
        let Some(ts) = parse_ts(f[3]) else { return };
        if ts < self.w.click_start || ts >= self.w.end {
            return;
        }
        let (mut ucheck, mut ccheck) = (0u16, 0u16);
        let user = make_key_check(f[1].bytes(), &mut ucheck);
        let cid = make_key_check(f[0].bytes(), &mut ccheck);
        let h = key_hash(user);
        let first = self.persons.first_stage(h);
        if is_long(user) {
            prefetch_long(user);
        }
        if is_long(cid) {
            prefetch_long(cid);
        }
        let i = st.nck % PARSE_DELAY;
        if st.nck >= PARSE_DELAY {
            self.flush_click(st, i);
        }
        if !cfg!(feature = "stage1") && st.nck >= LOOKUP_STAGE {
            let j = (st.nck - LOOKUP_STAGE) % PARSE_DELAY;
            let slot = self.persons.lookup_slot(st.pending_ck_h[j]);
            prefetch(slot);
            st.pending_ck_slot[j] = slot;
        }
        st.pending_ck[i] =
            PersonClick { person: user, cid, rel: E::rel((ts - self.w.click_start) as u64), camp };
        st.pending_ck_h[i] = h;
        st.pending_ck_slot[i] = first;
        st.pending_ck_user[i] = LongRef::of(f[1], ucheck);
        st.pending_ck_cid[i] = LongRef::of(f[0], ccheck);
        st.nck += 1;
    }
}

// ---------------------------------------------------------------- probe

// Analysis only (feature "probe"): the parse cut short at a layer, so the
// time of each layer is the difference to the one before. Results are folded
// into a black box so nothing is optimised away. Later stages see no input.
#[cfg(feature = "probe")]
mod probe {
    use super::*;
    pub const READ: u8 = 0; // read the bytes (pread pieces), nothing else
    pub const SCAN: u8 = 1; // + split lines into fields
    pub const FIELDS: u8 = 2; // + parse timestamps, type, price, campaign lookup
    pub const KEYS: u8 = 3; // + build the ID keys and hash them
    pub const RESOLVE: u8 = 4; // + each event's person, through the prefetch ring
    pub const FULL: u8 = 5;
    pub fn level() -> u8 {
        static L: std::sync::OnceLock<u8> = std::sync::OnceLock::new();
        *L.get_or_init(|| match std::env::var("SHOPPING_PROBE").as_deref() {
            Ok("read") => READ,
            Ok("scan") => SCAN,
            Ok("fields") => FIELDS,
            Ok("f-ingest") => 10,
            Ok("f-ts") => 11,
            Ok("f-rest") => 12,
            Ok("keys") => KEYS,
            Ok("resolve") => RESOLVE,
            _ => FULL,
        })
    }
    // SHOPPING_PROBE_SKIP letters skip parts of the full parse's filing:
    // d = the event_id records, e = the person events, c = the clicks,
    // r = recording long IDs. Output is wrong then; only the timing counts.
    pub fn skip(c: char) -> bool {
        static S: std::sync::OnceLock<String> = std::sync::OnceLock::new();
        S.get_or_init(|| std::env::var("SHOPPING_PROBE_SKIP").unwrap_or_default()).contains(c)
    }
    pub fn run(ctx: &Ctx, is_event: bool, d: *const u8, len: usize, s: usize, e: usize) {
        let lv = level();
        let mut acc = 0u64;
        let mut ring = [(0u64, 0u64); PARSE_DELAY];
        let mut nr = 0usize;
        if lv == READ {
            // touch one byte per cache line, as the scanner's loads would
            let mut i = s;
            while i < e {
                acc = acc.wrapping_add(unsafe { *d.add(i) } as u64);
                i += 64;
            }
        } else if is_event {
            for_lines::<7>(d, len, s, e, |f| {
                acc = acc.wrapping_add(f[0].n as u64);
                if lv < FIELDS {
                    return;
                }
                if lv >= 10 {
                    // one parser alone, on every line
                    acc = acc.wrapping_add(match lv {
                        10 => parse_f1(f[3]).unwrap_or(0) as u64,
                        11 => parse_ts(f[2]).unwrap_or(0) as u64,
                        _ => {
                            let ty = unsafe { load64(f[4].p) };
                            let price = parse_price(f[6]).unwrap_or(0);
                            let cat = if price != 0 { ctx.catalog.category(f[5]) } else { 0 };
                            ty ^ price as u64 ^ cat as u64
                        }
                    });
                    return;
                }
                let Some(ingest) = parse_f1(f[3]) else { return };
                let ts = parse_ts(f[2]).unwrap_or(0);
                let ty = unsafe { load64(f[4].p) };
                let price = parse_price(f[6]).unwrap_or(0);
                let cat = if price != 0 { ctx.catalog.category(f[5]) } else { 0 };
                acc = acc.wrapping_add(ingest as u64 ^ ts as u64 ^ ty ^ price as u64 ^ cat as u64);
                if lv < KEYS {
                    return;
                }
                let (mut c1, mut c2) = (0u16, 0u16);
                let eid = make_key_check(f[0].bytes(), &mut c1);
                let user = make_key_check(f[1].bytes(), &mut c2);
                let h = key_hash(user);
                acc = acc.wrapping_add(key_hash(eid) ^ h ^ c1 as u64 ^ c2 as u64);
                if lv < RESOLVE {
                    return;
                }
                ctx.persons.prefetch_hashed(h);
                let i = nr % PARSE_DELAY;
                if nr >= PARSE_DELAY {
                    let (u, uh) = ring[i];
                    let (person, linked) = ctx.persons.name_select(u, uh);
                    acc = acc.wrapping_add(key_hash(person) ^ linked as u64);
                }
                ring[i] = (user, h);
                nr += 1;
            });
        } else {
            for_lines::<4>(d, len, s, e, |f| {
                acc = acc.wrapping_add(f[0].n as u64);
                if lv < FIELDS {
                    return;
                }
                let camp = ctx.catalog.campaigns.find(make_key(f[2].bytes())).unwrap_or(0);
                let ts = parse_ts(f[3]).unwrap_or(0);
                acc = acc.wrapping_add(camp as u64 ^ ts as u64);
                if lv < KEYS {
                    return;
                }
                let (mut c1, mut c2) = (0u16, 0u16);
                let cid = make_key_check(f[0].bytes(), &mut c1);
                let user = make_key_check(f[1].bytes(), &mut c2);
                acc = acc.wrapping_add(key_hash(cid) ^ key_hash(user) ^ c1 as u64 ^ c2 as u64);
            });
        }
        std::hint::black_box(acc);
    }
}

// ---------------------------------------------------------------- pipeline

struct Pipeline {
    data_dir: String,
    out_dir: String,
    threads: usize,
    timer: Timer,
    w: Window,
}

struct DedupState {
    // entries: tag (32 hash bits) << 32 | record + 1; cleared per bucket (a
    // generation stamp instead was 27 ms slower on the Intel bench)
    table: Vec<u64>,
    blocks: Vec<*const DedupRec>,
    lens: Vec<u32>,
}
unsafe impl Send for DedupState {}

struct SessionState<E: Ev> {
    work: PersonWork<E>,
    local: KeyMap<u32>,
    names: Vec<Key>,
    cids: Vec<Key>,
    raw_cids: Vec<Key>,
    lids: Vec<u32>,
    tmp_lids: Vec<u32>,
    offsets: Vec<u32>,
    pos: Vec<u32>,
    ck_lids: Vec<u32>,
    ck_offsets: Vec<u32>,
    camps: Vec<u32>,
    raw_camps: Vec<u32>,
    hist: Vec<u32>,
    raw: Vec<E>,
    tmp: Vec<E>,
    evs: Vec<E>,
    cks: Vec<E>,
    ck_raw: Vec<E>,
}

impl Pipeline {
    fn read_manifest(&mut self) {
        let m = map_input(&format!("{}/manifest.json", self.data_dir));
        let json = unsafe { std::slice::from_raw_parts(m.data, m.len) }.to_vec();
        m.unmap();
        let find = |from: usize, pat: &[u8]| -> Option<usize> {
            json.get(from..)?.windows(pat.len()).position(|w| w == pat).map(|i| i + from)
        };
        let get = |name: &str| -> i64 {
            let key = format!("\"{name}\"");
            let v = (|| {
                let k = find(0, key.as_bytes())?;
                let colon = find(k + key.len(), b":")?;
                let a = find(colon + 1, b"\"")?;
                let b = find(a + 1, b"\"")?;
                let mut padded = json[a + 1..b].to_vec();
                let n = padded.len();
                padded.resize(n + 64, 0);
                parse_ts(Field { p: padded.as_ptr(), n })
            })();
            v.unwrap_or_else(|| die(&format!("bad manifest field {name}")))
        };
        let w = &mut self.w;
        w.start = get("window_start");
        w.end = get("window_end");
        w.click_start = w.start - WEEK;
        w.day0 = floor_div(w.start, DAY);
        w.days = if w.end > w.start { (floor_div(w.end - 1, DAY) - w.day0 + 1) as u32 } else { 1 };
    }

    // The long-ID table (base.rs), sized for about 1.5x the long IDs to be
    // recorded if 1 in 8 IDs is long: click IDs, link IDs and unlinked users.
    // SHOPPING_LONG_SLOTS overrides, as a restart that found it too full sets it.
    fn reserve_long_ids(ev_bytes: usize, ck_bytes: usize, link_bytes: usize) {
        if LONG_IDS.exact() {
            return;
        }
        // click lines ~50 bytes, link lines ~26 with 2 IDs each of ~1.4
        // uses, unlinked users ~1 in 256 event lines (~80 bytes)
        let want = ck_bytes / 50 / 8 + link_bytes / 26 / 6 + ev_bytes / 80 / 256 + 4096;
        let mut slots = pow2_at_least(want + want / 2);
        if let Some(s) = std::env::var("SHOPPING_LONG_SLOTS").ok().and_then(|s| s.parse::<usize>().ok()) {
            slots = pow2_at_least(s.max(16));
        }
        LONG_IDS.reserve(slots);
    }
    // A long-ID key collision reruns with exact IDs, a full table with one four times the size.
    fn check_long_ids(&self) {
        let t = &LONG_IDS;
        if t.collided.load(Relaxed) {
            restart("SHOPPING_EXACT_IDS", "1");
        }
        if t.full.load(Relaxed) {
            restart("SHOPPING_LONG_SLOTS", &(t.capacity() * 4).to_string());
        }
        if self.timer.on && !t.exact() {
            eprintln!("[{:>8}] long IDs recorded: {} of {} slots", "", t.used(), t.capacity());
        }
    }

    fn run(&mut self) {
        self.read_manifest();
        let dd = self.data_dir.clone();
        let catalog_thread = std::thread::spawn(move || Catalog::new(&dd));
        let dir = |name: &str| csv_files(&format!("{}/{name}", self.data_dir));
        let link_files: Vec<Input> = dir("links").iter().map(|p| map_input(p)).collect();
        // events and clicks are read with pread in L2-sized pieces (on a VM that
        // can beat faulting and walking 4 KB file pages); SHOPPING_MMAP=1 maps them
        let mapped = std::env::var_os("SHOPPING_MMAP").is_some();
        let open = |name: &str| -> Vec<Input> {
            dir(name).iter().map(|p| if mapped { map_input(p) } else { open_input(p) }).collect()
        };
        let event_files = open("events");
        let click_files = open("clicks");
        let ev_bytes: usize = event_files.iter().map(|f| f.len).sum();
        let ck_bytes: usize = click_files.iter().map(|f| f.len).sum();
        RESERVATION.reserve(2 * ev_bytes + 2 * ck_bytes + (self.threads << 28) + (256 << 20));
        let link_bytes: usize = link_files.iter().map(|f| f.len).sum();
        Self::reserve_long_ids(ev_bytes, ck_bytes, link_bytes);
        self.timer.lap("setup");

        // links
        let mut chunks = Vec::new();
        plan_chunks(&link_files, 1 << 20, &mut chunks);
        let mut link_pairs: Vec<Vec<(Key, Key)>> =
            (0..self.threads).map(|_| Vec::with_capacity(link_bytes / 18 / self.threads + 1024)).collect();
        run_tasks(&mut link_pairs, chunks.len(), |out, i| {
            let c = &chunks[i];
            for_lines::<2>(c.file.data, c.file.len, c.s, c.e, |f| {
                if !f[0].is_empty() && !f[1].is_empty() {
                    out.push((record_key(f[0].bytes()), record_key(f[1].bytes())));
                }
            });
        });
        drop(chunks);
        let catalog = catalog_thread.join().unwrap_or_else(|_| die("catalog failed"));
        self.check_long_ids(); // the persons build reads link IDs' bytes
        self.timer.lap("links");

        // events are packed in 64 bits when the window, prices and categories fit
        let small = catalog.categories.len() <= 256
            && self.w.end - self.w.click_start < (1i64 << 32)
            && std::env::var_os("SHOPPING_WIDE").is_none(); // forces the general path, for tests
        if small {
            self.process::<u64>(&catalog, link_pairs, link_files, event_files, click_files, ev_bytes);
        } else {
            self.process::<u128>(&catalog, link_pairs, link_files, event_files, click_files, ev_bytes);
        }
    }

    fn process<E: Ev>(
        &mut self,
        catalog: &Catalog,
        mut link_pairs: Vec<Vec<(Key, Key)>>,
        link_files: Vec<Input>,
        event_files: Vec<Input>,
        click_files: Vec<Input>,
        ev_bytes: usize,
    ) {
        let threads = self.threads;
        let est_lines = ev_bytes / 72 + 1;
        let event_buckets = pow2_at_least(est_lines / EVENT_BUCKET_LINES).clamp(16, 4096);
        let person_buckets = pow2_at_least(est_lines / env_size("SHOPPING_PBE", PERSON_BUCKET_EVENTS)).clamp(16, 8192);
        let t0 = Instant::now();
        let persons = build_persons(&mut link_pairs, threads);
        if self.timer.on {
            eprintln!(
                "[{:>8}] persons built in {:.1} ms ({} ids)",
                "",
                t0.elapsed().as_secs_f64() * 1e3,
                persons.count
            );
        }
        let ctx = Ctx {
            w: self.w,
            catalog,
            persons: &persons,
            event_shift: 64 - event_buckets.trailing_zeros(),
            person_shift: 64 - person_buckets.trailing_zeros(),
        };
        let mut parsed: Vec<ParseState<E>> =
            (0..threads).map(|_| ParseState::new(event_buckets, person_buckets)).collect();
        {
            let mut chunks = Vec::new();
            plan_chunks(&event_files, 4 << 20, &mut chunks);
            let event_chunks = chunks.len();
            plan_chunks(&click_files, 4 << 20, &mut chunks);
            let ctx = &ctx;
            let read_piece = env_size("SHOPPING_PIECE", READ_PIECE).max(4096);
            run_tasks(&mut parsed, chunks.len(), |st, i| {
                let c = &chunks[i];
                let is_event = i < event_chunks;
                let parse_range = |st: &mut ParseState<E>, d: *const u8, len: usize, s: usize, e: usize| {
                    #[cfg(feature = "probe")]
                    if probe::level() != probe::FULL {
                        probe::run(ctx, is_event, d, len, s, e);
                        return;
                    }
                    if is_event {
                        for_lines::<7>(d, len, s, e, |f| ctx.parse_event(f, st));
                    } else {
                        for_lines::<4>(d, len, s, e, |f| ctx.parse_click(f, st));
                    }
                };
                let file = c.file;
                if !file.data.is_null() {
                    file.populate(c.s, c.e + 4096);
                    parse_range(st, file.data, file.len, c.s, c.e);
                } else {
                    // Read in pieces small enough to stay in L2 while parsed. A piece
                    // is read from one byte before it (to see whether it starts a
                    // line) through the end of the line that crosses its end.
                    let mut ps = c.s;
                    while ps < c.e {
                        let pe = c.e.min(ps + read_piece);
                        let from = ps.saturating_sub(1);
                        let mut to = file.len.min(pe + 4096);
                        loop {
                            let need = to - from + 64;
                            if st.buf.len() < need {
                                st.buf.resize(need, 0);
                            }
                            file.read_range(from, to, &mut st.buf);
                            let tail = pe - from - 1; // the piece's last byte, in buf
                            if to == file.len || st.buf[tail..to - from].contains(&b'\n') {
                                break;
                            }
                            to = file.len.min(to + (to - from)); // a very long line: read more
                        }
                        let d = st.buf.as_ptr();
                        parse_range(st, d, to - from, ps - from, pe - from);
                        ctx.drain(st); // pending lines point into buf
                        ps += read_piece;
                    }
                }
                ctx.drain(st); // nothing may stay pending once the chunk is done
                store_fence();
            });
        }
        for st in parsed.iter_mut() {
            // write out every bucket's partial group
            st.ev.flush();
            st.dd.flush();
            st.ck.flush();
        }
        self.timer.lap("parse");
        // the records hold keys, not pointers: release the inputs off the critical path
        std::thread::spawn(move || {
            for f in event_files.iter().chain(&click_files).chain(&link_files) {
                f.unmap();
            }
        });
        self.finish(catalog, &parsed, event_buckets, person_buckets);
    }

    fn finish<E: Ev>(&mut self, catalog: &Catalog, parsed: &[ParseState<E>], event_buckets: usize, person_buckets: usize) {
        let threads = self.threads;
        // ---- dedup: per event_id bucket the earliest copy wins; every other
        // copy's filed event (if it has one) is dropped where it lies
        let mut dedup: Vec<DedupState> =
            (0..threads).map(|_| DedupState { table: Vec::new(), blocks: Vec::new(), lens: Vec::new() }).collect();
        run_tasks(&mut dedup, event_buckets, |st, b| {
            st.blocks.clear();
            st.lens.clear();
            let mut n = 0;
            for p in parsed {
                for j in 0..p.dd.num_blocks(b) {
                    let (blk, len) = p.dd.block(b, j);
                    st.blocks.push(blk);
                    st.lens.push(len);
                }
                n += p.dd.size(b);
            }
            if n == 0 {
                return;
            }
            let cap = pow2_at_least(2 * n);
            let mask = cap - 1;
            st.table.clear();
            st.table.resize(cap, 0);
            let blocks = &st.blocks;
            let rec = |r: u64| -> &DedupRec { unsafe { &*blocks.get_unchecked((r >> 7) as usize).add((r & 127) as usize) } };
            for (bi, &blk) in st.blocks.iter().enumerate() {
                for i in 0..unsafe { *st.lens.get_unchecked(bi) } as usize {
                    let r = unsafe { &*blk.add(i) };
                    let rref = ((bi as u64) << 7) | i as u64;
                    let eid = r.eid;
                    let h = key_hash(eid);
                    let tag = h >> 32;
                    let entry = (tag << 32) | (rref + 1);
                    let mut s = h as usize & mask;
                    let table = st.table.as_mut_ptr(); // s is always masked into the table
                    loop {
                        let cur = unsafe { *table.add(s) };
                        if cur == 0 {
                            unsafe { *table.add(s) = entry };
                            break;
                        }
                        if (cur >> 32) == tag {
                            let o = rec((cur & 0xFFFFFFFF) - 1);
                            if o.eid == eid {
                                if o.check() != r.check() {
                                    LONG_IDS.collided.store(true, Relaxed);
                                }
                                let (ri, oi) = (r.ingest, o.ingest);
                                let drop = if ri < oi {
                                    // the new copy wins
                                    unsafe { *table.add(s) = entry };
                                    o.ev::<E>()
                                } else {
                                    r.ev::<E>()
                                };
                                if !drop.is_null() {
                                    unsafe { std::ptr::addr_of_mut!((*drop).ev).write_unaligned(E::DROPPED) };
                                }
                                break;
                            }
                        }
                        s = (s + 1) & mask;
                    }
                }
            }
        });
        self.timer.lap("dedup");
        self.check_long_ids(); // sessions and output read recorded bytes

        // ---- sessions per person bucket, biggest buckets first
        let ncamp = catalog.campaign_names.len() as u32;
        let ncat = catalog.categories.len() as u32;
        let mut order: Vec<(usize, u32)> = (0..person_buckets)
            .map(|p| (parsed.iter().map(|d| d.ev.size(p) + d.ck.size(p)).sum(), p as u32))
            .collect();
        order.sort_by(|a, b| b.0.cmp(&a.0));
        let kept: usize = parsed.iter().map(|d| (0..person_buckets).map(|p| d.ev.size(p)).sum::<usize>()).sum();
        let w = self.w;
        let mut sess: Vec<SessionState<E>> = (0..threads)
            .map(|_| {
                let mut work = PersonWork::new(w, ncamp, ncat);
                // about one user-day row per 8 kept events: reserve (untouched) room for 2x that
                work.rows.reserve(kept / threads / 4 + 1024);
                SessionState {
                    work,
                    local: KeyMap::new(1024),
                    names: Vec::new(),
                    cids: Vec::new(),
                    raw_cids: Vec::new(),
                    lids: Vec::new(),
                    tmp_lids: Vec::new(),
                    offsets: Vec::new(),
                    pos: Vec::new(),
                    ck_lids: Vec::new(),
                    ck_offsets: Vec::new(),
                    camps: Vec::new(),
                    raw_camps: Vec::new(),
                    hist: Vec::new(),
                    raw: Vec::new(),
                    tmp: Vec::new(),
                    evs: Vec::new(),
                    cks: Vec::new(),
                    ck_raw: Vec::new(),
                }
            })
            .collect();
        #[cfg(feature = "probe")]
        let stage_ns: [std::sync::atomic::AtomicU64; 6] = Default::default();
        #[cfg(feature = "probe")]
        let lap = {
            let stage_ns = &stage_ns;
            move |t: &mut Instant, k: usize| {
                let now = Instant::now();
                stage_ns[k].fetch_add((now - *t).as_nanos() as u64, Relaxed);
                *t = now;
            }
        };
        run_tasks(&mut sess, person_buckets, |st, task| {
            #[cfg(feature = "probe")]
            let mut t = Instant::now();
            let p = order[task].1 as usize;
            let n: usize = parsed.iter().map(|d| d.ev.size(p)).sum();
            if n == 0 {
                return;
            }
            st.local.reset(n / 12 + 16); // about 20 events per person: room without regrowing
            st.names.clear();
            st.offsets.clear();
            st.offsets.push(0);
            // Unchecked from here on: every index below is a count of what
            // was just written (k < n events, lid < names, pos within its
            // person's range), and the buffers are written before read.
            resize_uninit(&mut st.raw, n);
            resize_uninit(&mut st.lids, n);
            let mut k = 0;
            let mut max_hi = 0u64;
            for d in parsed {
                d.ev.for_each(p, |e| {
                    let ev = e.ev;
                    if ev == E::DROPPED {
                        return; // a later copy of an event (dedup)
                    }
                    let person = e.person;
                    let (names, offsets) = (&mut st.names, &mut st.offsets);
                    let lid = st.local.get_or_insert(person, || {
                        names.push(person);
                        offsets.push(0);
                        (names.len() - 1) as u32
                    });
                    unsafe {
                        *st.offsets.get_unchecked_mut(lid as usize + 1) += 1;
                        *st.raw.get_unchecked_mut(k) = ev;
                        *st.lids.get_unchecked_mut(k) = lid;
                    }
                    k += 1;
                    max_hi = max_hi.max(ev.hi());
                });
            }
            let n = k; // copies dropped by dedup are gone
            st.raw.truncate(n);
            st.lids.truncate(n);
            if n == 0 {
                return;
            }
            #[cfg(feature = "probe")]
            lap(&mut t, 0); // gather events into the person map
            // the whole bucket in time order; the stable placement keeps it per person
            radix_by_time(&mut st.raw, &mut st.lids, &mut st.tmp, &mut st.tmp_lids, max_hi, &mut st.hist);
            let people = st.names.len();
            for i in 0..people {
                st.offsets[i + 1] += st.offsets[i];
            }
            resize_uninit(&mut st.evs, n);
            st.pos.clear();
            st.pos.extend_from_slice(&st.offsets[..people]);
            for k in 0..n {
                unsafe {
                    let l = *st.lids.get_unchecked(k) as usize;
                    let q = st.pos.get_unchecked_mut(l);
                    *st.evs.get_unchecked_mut(*q as usize) = *st.raw.get_unchecked(k);
                    *q += 1;
                }
            }

            #[cfg(feature = "probe")]
            lap(&mut t, 1); // radix sort and place events
            // clicks of persons with events, likewise in time order; the
            // buffers are sized for every click of the bucket, then cut
            let mc: usize = parsed.iter().map(|d| d.ck.size(p)).sum();
            resize_uninit(&mut st.ck_raw, mc);
            resize_uninit(&mut st.ck_lids, mc);
            resize_uninit(&mut st.raw_cids, mc);
            resize_uninit(&mut st.raw_camps, mc);
            let mut m = 0usize;
            let mut max_hi = 0u64;
            for d in parsed {
                d.ck.for_each(p, |c| {
                    let Some(lid) = st.local.find(c.person) else { return };
                    let rel = E::rel_value(c.rel);
                    unsafe {
                        *st.ck_raw.get_unchecked_mut(m) = E::pair(rel, m as u64);
                        *st.ck_lids.get_unchecked_mut(m) = lid;
                        *st.raw_cids.get_unchecked_mut(m) = c.cid;
                        *st.raw_camps.get_unchecked_mut(m) = c.camp;
                    }
                    m += 1;
                    max_hi = max_hi.max(rel);
                });
            }
            st.ck_raw.truncate(m);
            st.ck_lids.truncate(m);
            #[cfg(feature = "probe")]
            lap(&mut t, 2); // gather clicks
            radix_by_time(&mut st.ck_raw, &mut st.ck_lids, &mut st.tmp, &mut st.tmp_lids, max_hi, &mut st.hist);
            st.ck_offsets.clear();
            st.ck_offsets.resize(people + 1, 0);
            for &l in &st.ck_lids {
                st.ck_offsets[l as usize + 1] += 1;
            }
            for i in 0..people {
                st.ck_offsets[i + 1] += st.ck_offsets[i];
            }
            resize_uninit(&mut st.cks, m);
            resize_uninit(&mut st.cids, m);
            resize_uninit(&mut st.camps, m);
            st.pos.clear();
            st.pos.extend_from_slice(&st.ck_offsets[..people]);
            for k in 0..m {
                unsafe {
                    let l = *st.ck_lids.get_unchecked(k) as usize;
                    let slot = st.pos.get_unchecked_mut(l);
                    let q = *slot as usize;
                    *slot += 1;
                    let x = *st.ck_raw.get_unchecked(k);
                    let idx = x.lo() as usize;
                    *st.cks.get_unchecked_mut(q) = E::pair(x.hi(), q as u64);
                    *st.cids.get_unchecked_mut(q) = *st.raw_cids.get_unchecked(idx);
                    *st.camps.get_unchecked_mut(q) = *st.raw_camps.get_unchecked(idx);
                }
            }
            #[cfg(feature = "probe")]
            lap(&mut t, 3); // radix sort and place clicks
            for l in 0..people {
                let (e0, e1) = (st.offsets[l] as usize, st.offsets[l + 1] as usize);
                let (c0, c1) = (st.ck_offsets[l] as usize, st.ck_offsets[l + 1] as usize);
                st.work.run(st.names[l], &mut st.evs[e0..e1], &mut st.cks[c0..c1], &st.cids, &st.camps);
            }
            #[cfg(feature = "probe")]
            lap(&mut t, 4); // per-person work
        });
        #[cfg(feature = "probe")]
        {
            let names = ["gather events", "sort+place events", "gather clicks", "sort+place clicks", "per-person work", ""];
            for (k, name) in names.iter().enumerate().take(5) {
                eprintln!("[{:>8}] sessions {:<18} {:7.1} ms (all threads)", "", name, stage_ns[k].load(Relaxed) as f64 / 1e6);
            }
        }
        self.timer.lap("sessions");

        // ---- output
        let ud_dir = format!("{}/user_daily", self.out_dir);
        let cd_dir = format!("{}/campaign_daily", self.out_dir);
        for dir in [&ud_dir, &cd_dir] {
            let _ = std::fs::create_dir_all(dir);
            if let Ok(rd) = std::fs::read_dir(dir) {
                for e in rd.flatten() {
                    if e.path().extension().is_some_and(|x| x == "parquet") {
                        let _ = std::fs::remove_file(e.path());
                    }
                }
            }
        }
        let total_rows: usize = sess.iter().map(|s| s.work.rows.len()).sum();
        let mut dummy = vec![(); threads];
        let sess = &sess;
        run_tasks(&mut dummy, sess.len() + 1, |_, i| {
            if i == sess.len() {
                write_campaigns(sess, catalog, &w, &format!("{cd_dir}/part-00000.parquet"));
                return;
            }
            let rows = &sess[i].work.rows;
            if rows.is_empty() && !(i == 0 && total_rows == 0) {
                return;
            }
            write_users(rows, catalog, &format!("{ud_dir}/part-{i:05}.parquet"));
        });
        self.timer.lap("output");
        if self.timer.on {
            let (faults, maxrss) = rusage();
            eprintln!("[{:>8}] faults={} maxrss={}MB threads={}", "", faults, maxrss / 1024, threads);
        }
    }
}

fn write_users(rows: &[UserRow], catalog: &Catalog, path: &str) {
    use parquet::{Column, Kind};
    let n = rows.len();
    let mut cols = vec![
        Column::new("user_id", Kind::String),
        Column::new("day", Kind::Date),
        Column::new("sessions", Kind::Int64),
        Column::new("events", Kind::Int64),
        Column::new("revenue_cents", Kind::Int64),
        Column::new("top_category", Kind::OptString),
    ];
    cols[0].values.reserve(n * 14);
    cols[1].values.reserve(n * 4);
    for c in &mut cols[2..5] {
        c.values.reserve(n * 8);
    }
    cols[5].present.reserve(n);
    for r in rows {
        cols[0].add_key(r.name);
        cols[1].add_i32(r.day);
        cols[2].add_i64(r.sessions as i64);
        cols[3].add_i64(r.events as i64);
        cols[4].add_i64(r.revenue as i64);
        let has = r.top != NO_CATEGORY;
        cols[5].present.push(has as u8);
        if has {
            cols[5].add_string(&catalog.categories[r.top as usize]);
        }
    }
    parquet::write_file(path, &cols, n);
}

fn write_campaigns<E: Ev>(sess: &[SessionState<E>], catalog: &Catalog, w: &Window, path: &str) {
    use parquet::{Column, Kind};
    let ncamp = catalog.campaign_names.len();
    let mut sum = vec![[0u64; 4]; (ncamp + 1) * w.days as usize];
    for s in sess {
        for (acc, c) in sum.iter_mut().zip(&s.work.cells.cells) {
            for k in 0..4 {
                acc[k] += c[k];
            }
        }
    }
    let mut cols = vec![
        Column::new("campaign_id", Kind::String),
        Column::new("channel", Kind::String),
        Column::new("day", Kind::Date),
        Column::new("sessions", Kind::Int64),
        Column::new("purchasing_sessions", Kind::Int64),
        Column::new("revenue_cents", Kind::Int64),
        Column::new("assisted_sessions", Kind::Int64),
    ];
    let mut rows = 0;
    for c in 0..=ncamp {
        for d in 0..w.days as usize {
            let v = sum[c * w.days as usize + d];
            if v[0] == 0 && v[3] == 0 {
                continue;
            }
            if c == ncamp {
                cols[0].add_string(b"organic");
                cols[1].add_string(b"organic");
            } else {
                cols[0].add_string(&catalog.campaign_names[c]);
                cols[1].add_string(&catalog.channels[c]);
            }
            cols[2].add_i32((w.day0 + d as i64) as i32);
            for k in 0..4 {
                cols[3 + k].add_i64(v[k] as i64);
            }
            rows += 1;
        }
    }
    parquet::write_file(path, &cols, rows);
}

// Minor faults and peak RSS (KB) of this process.
fn rusage() -> (i64, i64) {
    extern "C" {
        fn getrusage(who: i32, usage: *mut [i64; 18]) -> i32;
    }
    let mut u = [0i64; 18];
    unsafe { getrusage(0, &mut u) };
    (u[8], u[4]) // after two timevals: maxrss, ixrss, idrss, isrss, minflt
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    LONG_IDS.set_exact(std::env::var_os("SHOPPING_EXACT_IDS").is_some());
    let mut data = std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into());
    let mut out = std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into());
    // the fixture runner also passes DATA_DIR and OUTPUT_DIR as the last two arguments
    if args.len() >= 3 {
        data = args[args.len() - 2].clone();
        out = args[args.len() - 1].clone();
    }
    // NCPU, capped by what the process may actually run at once (affinity
    // mask and cgroup quota: available_parallelism reads both)
    let usable = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8);
    let ncpu = std::env::var("NCPU").ok().and_then(|s| s.parse::<usize>().ok()).unwrap_or(0);
    let threads = if ncpu > 0 { ncpu.min(usable) } else { usable }.clamp(1, 64);
    let mut p = Pipeline {
        data_dir: data,
        out_dir: out,
        threads,
        timer: Timer::new(std::env::var_os("SHOPPING_PROFILE").is_some()),
        w: Window::default(),
    };
    p.run();
    std::process::exit(0); // the kernel reclaims everything; skip destructors
}
