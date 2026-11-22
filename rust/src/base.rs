// Shared primitives: packed ID keys and the long-ID table, hashing,
// prefetch, a task-pulling thread helper, raw libc calls, and stage timing.
use std::cell::Cell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, AtomicUsize, Ordering::*};
use std::sync::Mutex;
use std::time::Instant;

pub type Key = u64;

pub fn die(msg: &str) -> ! {
    eprintln!("shopping-sessions: {msg}");
    std::process::exit(1)
}

// ---------------------------------------------------------------- libc

pub mod sys {
    use std::ffi::{c_int, c_long, c_void};
    extern "C" {
        pub fn mmap(addr: *mut c_void, len: usize, prot: c_int, flags: c_int, fd: c_int, off: i64) -> *mut c_void;
        pub fn munmap(addr: *mut c_void, len: usize) -> c_int;
        pub fn madvise(addr: *mut c_void, len: usize, advice: c_int) -> c_int;
        pub fn sysconf(name: c_int) -> c_long;
    }
    pub const PROT_READ: c_int = 1;
    pub const PROT_WRITE: c_int = 2;
    pub const MAP_PRIVATE: c_int = 2;
    pub const MAP_FIXED: c_int = 0x10;
    #[cfg(target_os = "linux")]
    pub const MAP_ANON: c_int = 0x20;
    #[cfg(not(target_os = "linux"))]
    pub const MAP_ANON: c_int = 0x1000;
    #[cfg(target_os = "linux")]
    pub const MAP_NORESERVE: c_int = 0x4000;
    #[cfg(not(target_os = "linux"))]
    pub const MAP_NORESERVE: c_int = 0x40;
    pub const MADV_HUGEPAGE: c_int = 14;
    pub const MADV_POPULATE_READ: c_int = 22;
    #[cfg(target_os = "linux")]
    pub const SC_PAGESIZE: c_int = 30;
    #[cfg(not(target_os = "linux"))]
    pub const SC_PAGESIZE: c_int = 29;
    pub const MAP_FAILED: *mut c_void = !0usize as *mut c_void;
}

pub const HUGE_PAGE: usize = 2 << 20;

// Anonymous zero pages, 2 MB aligned, huge pages asked for.
pub fn map_anon(bytes: usize) -> *mut u8 {
    let len = bytes.div_ceil(HUGE_PAGE) * HUGE_PAGE;
    unsafe {
        let p = sys::mmap(
            std::ptr::null_mut(),
            len + HUGE_PAGE,
            sys::PROT_READ | sys::PROT_WRITE,
            sys::MAP_PRIVATE | sys::MAP_ANON | sys::MAP_NORESERVE,
            -1,
            0,
        );
        if p == sys::MAP_FAILED {
            die(&format!("cannot map {len} bytes"));
        }
        let aligned = (p as usize + HUGE_PAGE - 1) & !(HUGE_PAGE - 1);
        #[cfg(target_os = "linux")]
        sys::madvise(aligned as *mut _, len, sys::MADV_HUGEPAGE);
        aligned as *mut u8
    }
}

// ---------------------------------------------------------------- loads

#[inline(always)]
pub unsafe fn load64(p: *const u8) -> u64 {
    u64::from_le(std::ptr::read_unaligned(p as *const u64))
}
#[inline(always)]
pub unsafe fn load32(p: *const u8) -> u32 {
    u32::from_le(std::ptr::read_unaligned(p as *const u32))
}

#[inline(always)]
pub fn prefetch<T>(p: *const T) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::x86_64::_mm_prefetch(p as *const i8, std::arch::x86_64::_MM_HINT_T0);
    }
    #[cfg(target_arch = "aarch64")]
    unsafe {
        std::arch::asm!("prfm pldl1keep, [{0}]", in(reg) p, options(nostack, readonly, preserves_flags));
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    let _ = p;
}

// ---------------------------------------------------------------- fields

// A field of a line: a view into a padded input buffer (fixed-width loads
// may run past its end). Valid while the buffer it points into is.
#[derive(Clone, Copy)]
pub struct Field {
    pub p: *const u8,
    pub n: usize,
}
// Padding an empty field can point at (fixed-width loads read up to 64 bytes).
static EMPTY_PAD: [u8; 64] = [0; 64];

impl Field {
    pub const EMPTY: Field = Field { p: EMPTY_PAD.as_ptr(), n: 0 };
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.n
    }
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.n == 0
    }
    // p is never null: the scanner's fields point into their line, and
    // EMPTY into EMPTY_PAD, so no branch is needed for n == 0.
    #[inline(always)]
    pub fn bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.p, self.n) }
    }
    // Byte i, which may be past the end (padding).
    #[inline(always)]
    pub unsafe fn at(&self, i: usize) -> u8 {
        *self.p.add(i)
    }
}

// ---------------------------------------------------------------- keys
//
// An ID of 1..10 bytes from [0-9A-Z_a-z] becomes a u64: each byte a 6-bit
// code (1..63, in ASCII order; 0 pads), the first byte in the top bits of the
// low 60. Integer order is byte-wise order (padding sorts first, as a prefix
// does) and equal integers mean equal strings; zero is never a key.
//
// Every other ID is long: about 1 in 8 of the generator's IDs (those with a
// 32-hex-digit suffix), and any ID with another byte. A long ID is keyed
// LONG | 63 bits of a hash of its bytes, computed where it is read, with no
// shared state. Two distinct long IDs share a key with probability about
// n^2 / 2^64. Where a long ID's bytes are needed (person names, click_id
// ties, output), they are also filed in one insert-only table with 16 more
// hash bits: a key shared by two IDs is noticed (collided), and so is a
// table too full to take more (full). Either restarts the run (main.rs):
// a fuller table gets a bigger one, a collision gets SHOPPING_EXACT_IDS=1,
// where every long ID is interned under a lock and keyed by its index.
pub const LONG: Key = 1 << 63;

const fn make_codes() -> ([u8; 256], [u8; 64]) {
    let mut code = [0xFFu8; 256];
    let mut byte = [0u8; 64];
    let mut next = 1u8;
    let mut c = 0usize;
    while c < 256 {
        let ch = c as u8;
        let ok = ch.is_ascii_digit() || ch.is_ascii_uppercase() || ch == b'_' || ch.is_ascii_lowercase();
        if ok {
            code[c] = next;
            byte[next as usize] = ch;
            next += 1;
        }
        c += 1;
    }
    (code, byte)
}
const CODES: ([u8; 256], [u8; 64]) = make_codes();
pub static KEY_CODE: [u8; 256] = CODES.0;
pub static KEY_BYTE: [u8; 64] = CODES.1;
const _: () = assert!(CODES.0[b'z' as usize] == 63 && CODES.0[b'0' as usize] == 1);

#[inline(always)]
pub fn mum(a: u64, b: u64) -> u64 {
    let r = (a as u128).wrapping_mul(b as u128);
    (r as u64) ^ ((r >> 64) as u64)
}

// Two independent 64-bit hashes of the bytes.
pub fn id_hash(s: &[u8]) -> (u64, u64) {
    const K1: u64 = 0xA0761D6478BD642F;
    const K2: u64 = 0xE7037ED1A0B428DB;
    let n = s.len();
    let mut a = 0x243F6A8885A308D3u64 ^ n as u64;
    let mut b = 0x13198A2E03707344u64 ^ ((n as u64) << 32);
    let mut i = 0;
    while i + 8 <= n {
        let w = u64::from_le_bytes(s[i..i + 8].try_into().unwrap());
        a = mum(a ^ w, K1);
        b = mum(b ^ w, K2);
        i += 8;
    }
    if i < n {
        let mut t = [0u8; 8];
        t[..n - i].copy_from_slice(&s[i..]);
        let w = u64::from_le_bytes(t);
        a = mum(a ^ w, K1);
        b = mum(b ^ w, K2);
    }
    (mum(a ^ 0x8EBC6AF09C88C6E3, 0x589965CC75374CC3), mum(b ^ 0x1D8E4E27C47D124F, 0xEB44ACCAB455D165))
}

#[repr(C)]
pub struct Slot {
    pub key: AtomicU64,
    pub val: AtomicU64, // the bytes (u32 length, then the ID) | 16 check bits << 48
}
pub const PTR_MASK: u64 = (1 << 48) - 1;

struct Exact {
    index: HashMap<Vec<u8>, u32>,
    names: Vec<Box<[u8]>>,
}

pub struct LongIds {
    slots: AtomicPtr<Slot>,
    mask: AtomicUsize,
    pub collided: AtomicBool,
    pub full: AtomicBool,
    exact: AtomicBool,
    exact_ids: Mutex<Option<Exact>>,
}
pub static LONG_IDS: LongIds = LongIds {
    slots: AtomicPtr::new(std::ptr::null_mut()),
    mask: AtomicUsize::new(0),
    collided: AtomicBool::new(false),
    full: AtomicBool::new(false),
    exact: AtomicBool::new(false),
    exact_ids: Mutex::new(None),
};

impl LongIds {
    // Called once, before any thread starts.
    pub fn set_exact(&self, on: bool) {
        self.exact.store(on, Relaxed);
    }
    #[inline(always)]
    pub fn exact(&self) -> bool {
        self.exact.load(Relaxed)
    }
    // n slots (a power of two), zero pages until touched.
    pub fn reserve(&self, n: usize) {
        let p = map_anon(n * std::mem::size_of::<Slot>()) as *mut Slot;
        self.slots.store(p, Relaxed);
        self.mask.store(n - 1, Relaxed);
    }
    pub fn capacity(&self) -> usize {
        if self.slots.load(Relaxed).is_null() {
            0
        } else {
            self.mask.load(Relaxed) + 1
        }
    }
    #[inline(always)]
    fn slot(&self, i: usize) -> &Slot {
        unsafe { &*self.slots.load(Relaxed).add(i) }
    }
    pub fn used(&self) -> usize {
        (0..self.capacity()).filter(|&i| self.slot(i).key.load(Relaxed) != 0).count()
    }
}

// Recorded bytes live in per-thread 1 MB chunks that are never freed.
thread_local! {
    static ARENA: Cell<(usize, usize)> = const { Cell::new((0, 0)) };
}
fn store_id(s: &[u8]) -> *const u8 {
    let need = (4 + s.len() + 7) & !7;
    ARENA.with(|a| {
        let (mut cur, mut end) = a.get();
        if end - cur < need {
            let size = need.max(1 << 20);
            let layout = std::alloc::Layout::from_size_align(size, 64).unwrap();
            cur = unsafe { std::alloc::alloc(layout) } as usize;
            if cur == 0 || ((cur + size) >> 48) != 0 {
                die("cannot store long IDs");
            }
            end = cur + size;
        }
        let p = cur as *mut u8;
        unsafe {
            std::ptr::write_unaligned(p as *mut u32, s.len() as u32);
            std::ptr::copy_nonoverlapping(s.as_ptr(), p.add(4), s.len());
        }
        a.set((cur + need, end));
        p as *const u8
    })
}

// Files a long ID's bytes (key and check from make_key_check). A no-op in exact mode.
pub fn record_long(k: Key, check: u16, s: &[u8]) {
    let t = &LONG_IDS;
    if t.exact() {
        return;
    }
    let mask = t.mask.load(Relaxed);
    let mut i = k as usize & mask;
    for _ in 0..256 {
        let slot = t.slot(i);
        let mut cur = slot.key.load(Relaxed);
        if cur == 0 {
            match slot.key.compare_exchange(0, k, Relaxed, Relaxed) {
                Ok(_) => {
                    slot.val.store(store_id(s) as u64 | (check as u64) << 48, Release);
                    return;
                }
                Err(other) => cur = other,
            }
        }
        if cur == k {
            let mut v = slot.val.load(Acquire);
            while v == 0 {
                std::hint::spin_loop();
                v = slot.val.load(Acquire);
            }
            if (v >> 48) as u16 != check {
                t.collided.store(true, Relaxed);
            }
            return;
        }
        i = (i + 1) & mask;
    }
    t.full.store(true, Relaxed);
}

#[inline(always)]
pub fn prefetch_long(k: Key) {
    let t = &LONG_IDS;
    let p = t.slots.load(Relaxed);
    if !p.is_null() {
        prefetch(unsafe { p.add(k as usize & t.mask.load(Relaxed)) });
    }
}

#[inline(never)]
fn intern_exact(s: &[u8]) -> Key {
    let mut g = LONG_IDS.exact_ids.lock().unwrap();
    let e = g.get_or_insert_with(|| Exact { index: HashMap::new(), names: Vec::new() });
    if let Some(&i) = e.index.get(s) {
        return LONG | i as u64;
    }
    let i = e.names.len() as u32;
    e.names.push(s.to_vec().into_boxed_slice());
    e.index.insert(s.to_vec(), i);
    LONG | i as u64
}

#[inline(never)]
pub fn long_key(s: &[u8], check: &mut u16) -> Key {
    if LONG_IDS.exact() {
        return intern_exact(s);
    }
    #[allow(unused_mut)]
    let (mut h1, h2) = id_hash(s);
    #[cfg(feature = "collide")]
    {
        h1 &= (1 << 13) - 1; // tests: few key bits, so long IDs collide
    }
    *check = h2 as u16;
    LONG | (h1 >> 1)
}

// The packed key, if the ID packs.
#[inline(always)]
fn pack(s: &[u8]) -> Option<Key> {
    let n = s.len();
    if n > 10 {
        return None;
    }
    let mut v = 0u64;
    let mut bad = 0u32;
    for i in 0..10 {
        let c = if i < n { KEY_CODE[unsafe { *s.get_unchecked(i) } as usize] as u32 } else { 0 };
        bad |= c;
        v = (v << 6) | (c & 63) as u64;
    }
    if bad & 0xC0 == 0 {
        Some(v)
    } else {
        None
    }
}

// make_key: IDs only compared for equality (event_id, product, campaign
// lookups); record_key: IDs whose bytes may be needed later;
// make_key_check: as make_key, with what a later record_long needs.
#[inline(always)]
pub fn make_key(s: &[u8]) -> Key {
    let mut c = 0;
    make_key_check(s, &mut c)
}
#[inline(always)]
pub fn make_key_check(s: &[u8], check: &mut u16) -> Key {
    match pack(s) {
        Some(k) => k,
        None => long_key(s, check),
    }
}
#[inline(always)]
pub fn record_key(s: &[u8]) -> Key {
    let mut c = 0;
    let k = make_key_check(s, &mut c);
    if is_long(k) {
        record_long(k, c, s);
    }
    k
}

#[inline(always)]
pub fn is_long(k: Key) -> bool {
    k & LONG != 0
}

// A packed key's bytes into out (at most 10); returns their count.
#[inline]
pub fn packed_bytes(k: Key, out: &mut [u8; 16]) -> usize {
    let mut n = 0;
    for i in 0..10 {
        let c = ((k >> (54 - 6 * i)) & 63) as usize;
        if c == 0 {
            break;
        }
        out[n] = KEY_BYTE[c];
        n += 1;
    }
    n
}

// A recorded long ID's bytes (once the threads recording it have joined).
pub fn long_bytes(k: Key) -> &'static [u8] {
    let t = &LONG_IDS;
    if t.exact() {
        let g = t.exact_ids.lock().unwrap();
        let b: &[u8] = &g.as_ref().unwrap().names[(k as u32) as usize];
        // names are boxed and never freed
        return unsafe { std::slice::from_raw_parts(b.as_ptr(), b.len()) };
    }
    let mask = t.mask.load(Relaxed);
    let mut i = k as usize & mask;
    loop {
        let s = t.slot(i);
        let key = s.key.load(Relaxed);
        if key == k {
            let p = (s.val.load(Relaxed) & PTR_MASK) as *const u8;
            unsafe {
                let n = std::ptr::read_unaligned(p as *const u32) as usize;
                return std::slice::from_raw_parts(p.add(4), n);
            }
        }
        if key == 0 {
            die("long ID used before it was recorded");
        }
        i = (i + 1) & mask;
    }
}

// Any key's bytes; buf holds a packed key's.
#[inline]
pub fn key_bytes(k: Key, buf: &mut [u8; 16]) -> &[u8] {
    if is_long(k) {
        return long_bytes(k);
    }
    let n = packed_bytes(k, buf);
    &buf[..n]
}

#[inline(never)]
pub fn key_less_slow(a: Key, b: Key) -> bool {
    if a == b {
        return false;
    }
    let (mut x, mut y) = ([0u8; 16], [0u8; 16]);
    key_bytes(a, &mut x) < key_bytes(b, &mut y)
}
#[inline(always)]
pub fn key_less(a: Key, b: Key) -> bool {
    if (a | b) & LONG != 0 {
        return key_less_slow(a, b);
    }
    a < b
}

#[inline(always)]
pub fn mix64(mut x: u64) -> u64 {
    x ^= x >> 32;
    x = x.wrapping_mul(0xd6e8feb86659fd93);
    x ^= x >> 32;
    x = x.wrapping_mul(0xd6e8feb86659fd93);
    x ^= x >> 32;
    x
}
#[inline(always)]
pub fn key_hash(k: Key) -> u64 {
    mix64(k.wrapping_mul(0x9E3779B97F4A7C15))
}

// ---------------------------------------------------------------- vectors

// Sets v's length to n without initialising new elements: for plain-integer
// buffers that are fully written before they are read.
#[inline(always)]
pub fn resize_uninit<T: Copy>(v: &mut Vec<T>, n: usize) {
    v.clear();
    v.reserve(n);
    unsafe { v.set_len(n) };
}

// ---------------------------------------------------------------- threads

// A pointer shared by workers that write disjoint parts of what it points at.
#[derive(Clone, Copy)]
pub struct Shared<T>(pub *mut T);
unsafe impl<T> Send for Shared<T> {}
unsafe impl<T> Sync for Shared<T> {}
impl<T> Shared<T> {
    #[inline(always)]
    pub unsafe fn at(&self, i: usize) -> &mut T {
        &mut *self.0.add(i)
    }
}

// Runs states.len() workers; each owns one state and pulls task indexes
// 0..tasks-1 from a shared counter.
pub fn run_tasks<S: Send, F: Fn(&mut S, usize) + Sync>(states: &mut [S], tasks: usize, work: F) {
    let next = AtomicUsize::new(0);
    let body = |st: &mut S| loop {
        let i = next.fetch_add(1, Relaxed);
        if i >= tasks {
            break;
        }
        work(st, i);
    };
    std::thread::scope(|sc| {
        let (first, rest) = states.split_first_mut().expect("no workers");
        for st in rest {
            sc.spawn(|| body(st));
        }
        body(first);
    });
}

// ---------------------------------------------------------------- timing

pub struct Timer {
    pub on: bool,
    t0: Instant,
    last: Instant,
}
impl Timer {
    pub fn new(on: bool) -> Timer {
        let now = Instant::now();
        Timer { on, t0: now, last: now }
    }
    pub fn lap(&mut self, what: &str) {
        if !self.on {
            return;
        }
        let now = Instant::now();
        eprintln!(
            "[{:8.1} ms] {:<24} {:8.1} ms",
            (now - self.t0).as_secs_f64() * 1e3,
            what,
            (now - self.last).as_secs_f64() * 1e3
        );
        self.last = now;
    }
}
