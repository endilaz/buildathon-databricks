// Input mappings (padded so fixed-width loads may run past any field), the
// allocator, and the bump arena behind the radix-scatter buckets.
use crate::base::*;
use std::alloc::{GlobalAlloc, Layout, System};
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::os::unix::io::AsRawFd;
use std::sync::atomic::{AtomicUsize, Ordering::*};

pub struct Input {
    pub data: *const u8, // mapped bytes, or null for a file read with pread
    pub len: usize,
    mapped: usize,
    file: Option<File>, // open for pread (opened, not mapped)
}
unsafe impl Send for Input {}
unsafe impl Sync for Input {}

pub fn page_size() -> usize {
    unsafe { sys::sysconf(sys::SC_PAGESIZE) as usize }
}

fn open(path: &str) -> (File, usize) {
    let f = File::open(path).unwrap_or_else(|_| die(&format!("cannot open {path}")));
    let len = f.metadata().unwrap_or_else(|_| die(&format!("cannot stat {path}"))).len() as usize;
    (f, len)
}

// Maps a file read-only with at least one zero page after its bytes.
pub fn map_input(path: &str) -> Input {
    let (f, len) = open(path);
    let page = page_size();
    let total = len.div_ceil(page) * page + page;
    unsafe {
        let base = sys::mmap(std::ptr::null_mut(), total, sys::PROT_READ, sys::MAP_PRIVATE | sys::MAP_ANON, -1, 0);
        if base == sys::MAP_FAILED {
            die(&format!("cannot reserve {total} bytes for {path}"));
        }
        if len > 0 {
            let p = sys::mmap(base, len, sys::PROT_READ, sys::MAP_PRIVATE | sys::MAP_FIXED, f.as_raw_fd(), 0);
            if p == sys::MAP_FAILED {
                die(&format!("cannot map {path}"));
            }
        }
        Input { data: base as *const u8, len, mapped: total, file: None }
    }
}

// Opens a file for pread without mapping it.
pub fn open_input(path: &str) -> Input {
    let (f, len) = open(path);
    Input { data: std::ptr::null(), len, mapped: 0, file: Some(f) }
}

impl Input {
    // Reads [from, to) of a file opened with open_input into buf (which
    // must hold to - from + 64 bytes); the 64 bytes after the data are zeroed.
    pub fn read_range(&self, from: usize, to: usize, buf: &mut [u8]) {
        let f = self.file.as_ref().unwrap();
        let want = to - from;
        let mut done = 0;
        while done < want {
            match f.read_at(&mut buf[done..want], (from + done) as u64) {
                Ok(r) if r > 0 => done += r,
                _ => die("cannot read input"),
            }
        }
        buf[want..want + 64].fill(0);
    }
    pub fn unmap(&self) {
        if !self.data.is_null() {
            unsafe { sys::munmap(self.data as *mut _, self.mapped) };
        }
    }
    // Faults in the page-cache pages of [s, e) with one call (Linux 5.14+).
    pub fn populate(&self, s: usize, e: usize) {
        #[cfg(target_os = "linux")]
        unsafe {
            let a = (self.data as usize + s) & !4095;
            let b = self.data as usize + e.min(self.len);
            if b > a {
                sys::madvise(a as *mut _, b - a, sys::MADV_POPULATE_READ);
            }
        }
        #[cfg(not(target_os = "linux"))]
        let _ = (s, e);
    }
}

pub fn csv_files(dir: &str) -> Vec<String> {
    let mut out = Vec::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            if p.extension().is_some_and(|x| x == "csv") && std::fs::metadata(&p).is_ok_and(|m| m.is_file()) {
                out.push(p.to_string_lossy().into_owned());
            }
        }
    }
    out.sort();
    out
}

// ---------------------------------------------------------------- allocator

// Allocations of 1 MB or more get their own 2 MB-aligned mapping with huge
// pages asked for (so first touches fault in 2 MB at a time, not 4 KB), and
// come zeroed for free. A 64-byte header in front records the mapping.
pub const BIG_ALLOC: usize = 1 << 20;

unsafe fn big_alloc(n: usize) -> *mut u8 {
    let len = (n + 64).div_ceil(HUGE_PAGE) * HUGE_PAGE + HUGE_PAGE;
    let raw = sys::mmap(
        std::ptr::null_mut(),
        len,
        sys::PROT_READ | sys::PROT_WRITE,
        sys::MAP_PRIVATE | sys::MAP_ANON | sys::MAP_NORESERVE,
        -1,
        0,
    );
    if raw == sys::MAP_FAILED {
        return std::ptr::null_mut();
    }
    let aligned = (raw as usize + HUGE_PAGE - 1) & !(HUGE_PAGE - 1);
    #[cfg(target_os = "linux")]
    sys::madvise(aligned as *mut _, raw as usize + len - aligned, sys::MADV_HUGEPAGE);
    let h = aligned as *mut usize;
    *h = raw as usize;
    *h.add(1) = len;
    (aligned + 64) as *mut u8
}
unsafe fn big_free(p: *mut u8) {
    let h = (p as usize - 64) as *const usize;
    sys::munmap(*h as *mut _, *h.add(1));
}

pub struct Alloc;
#[inline(always)]
fn is_big(l: &Layout) -> bool {
    l.size() >= BIG_ALLOC && l.align() <= 64
}
unsafe impl GlobalAlloc for Alloc {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        if is_big(&l) {
            big_alloc(l.size())
        } else {
            System.alloc(l)
        }
    }
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        if is_big(&l) {
            big_alloc(l.size()) // fresh anonymous pages are zero
        } else {
            System.alloc_zeroed(l)
        }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        if is_big(&l) {
            big_free(p)
        } else {
            System.dealloc(p, l)
        }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new_size: usize) -> *mut u8 {
        let nl = Layout::from_size_align_unchecked(new_size, l.align());
        if !is_big(&l) && !is_big(&nl) {
            return System.realloc(p, l, new_size);
        }
        let q = self.alloc(nl);
        if !q.is_null() {
            std::ptr::copy_nonoverlapping(p, q, l.size().min(new_size));
            self.dealloc(p, l);
        }
        q
    }
}

// ---------------------------------------------------------------- arena

// One address-space reservation shared by every arena and cut into slabs
// with an atomic bump, so the hot phases make no mmap calls.
pub struct Reservation {
    next: AtomicUsize,
    end: AtomicUsize,
}
pub static RESERVATION: Reservation = Reservation { next: AtomicUsize::new(0), end: AtomicUsize::new(0) };
impl Reservation {
    pub fn reserve(&self, bytes: usize) {
        let p = map_anon(bytes) as usize;
        self.next.store(p, Relaxed);
        self.end.store(p + bytes, Relaxed);
    }
}

pub struct Arena {
    cur: usize,
    end: usize,
}
const SLAB: usize = 2 << 20;
impl Arena {
    pub fn new() -> Arena {
        Arena { cur: 0, end: 0 }
    }
    pub fn alloc(&mut self, bytes: usize) -> *mut u8 {
        let mut p = (self.cur + 63) & !63;
        if p + bytes > self.end {
            let mut size = SLAB.max((bytes + 64).div_ceil(SLAB) * SLAB);
            let r = &RESERVATION;
            let mut s = r.next.fetch_add(size, Relaxed);
            if s == 0 || s + size > r.end.load(Relaxed) {
                size = size.max(64 << 20);
                s = map_anon(size) as usize;
            }
            self.cur = s;
            self.end = s + size;
            p = (self.cur + 63) & !63;
        }
        self.cur = p + bytes;
        p as *mut u8
    }
}

// Radix-scatter buffer: per bucket, a list of fixed-size blocks from the
// owner's arena. A push is a store and a counter bump; nothing is copied.
pub const BLOCK: u32 = 128;

#[derive(Clone, Copy)]
struct Cursor<T> {
    cur: *mut T,
    fill: u32,
    staged: u32,
}

pub struct Scatter<T: Copy> {
    arena: Arena,
    cursors: Vec<Cursor<T>>,
    blocks: Vec<Vec<*mut T>>,
}
unsafe impl<T: Copy> Send for Scatter<T> {}
unsafe impl<T: Copy> Sync for Scatter<T> {}

impl<T: Copy> Scatter<T> {
    pub fn new(n: usize) -> Self {
        Scatter {
            arena: Arena::new(),
            cursors: vec![Cursor { cur: std::ptr::null_mut(), fill: BLOCK, staged: 0 }; n],
            blocks: vec![Vec::new(); n],
        }
    }
    #[inline(never)]
    fn grow(&mut self, i: usize) {
        let p = self.arena.alloc(std::mem::size_of::<T>() * BLOCK as usize) as *mut T;
        self.cursors[i] = Cursor { cur: p, fill: 0, staged: 0 };
        self.blocks[i].push(p);
    }
    #[inline(always)]
    pub fn push(&mut self, i: usize, x: T) {
        if self.cursors[i].fill == BLOCK {
            self.grow(i);
        }
        let c = &mut self.cursors[i];
        unsafe { c.cur.add(c.fill as usize).write(x) };
        c.fill += 1;
    }
    pub fn size(&self, i: usize) -> usize {
        let n = self.blocks[i].len();
        if n == 0 {
            0
        } else {
            (n - 1) * BLOCK as usize + self.cursors[i].fill as usize
        }
    }
    pub fn for_each(&self, i: usize, mut f: impl FnMut(&T)) {
        let b = &self.blocks[i];
        for (j, &p) in b.iter().enumerate() {
            let n = if j + 1 == b.len() { self.cursors[i].fill } else { BLOCK };
            for k in 0..n as usize {
                f(unsafe { &*p.add(k) });
            }
        }
    }
}

// Orders this thread's non-temporal stores before anything it does next.
#[inline(always)]
pub fn store_fence() {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::x86_64::_mm_sfence()
    };
}

// A Scatter whose pushes go through a small staging group per bucket (write
// combining): a full group of GROUP records is copied to the bucket's block
// with non-temporal stores on x86, so writing the blocks costs no reads of
// their lines. A record's final address is known when it is staged. Call
// flush() once the pushes are done and before anything reads the buckets.
pub const GROUP: u32 = 8;

pub struct StagedScatter<T: Copy> {
    arena: Arena,
    cursors: Vec<Cursor<T>>,
    stage: Vec<T>, // GROUP records per bucket
    blocks: Vec<Vec<*mut T>>,
}
unsafe impl<T: Copy> Send for StagedScatter<T> {}
unsafe impl<T: Copy> Sync for StagedScatter<T> {}

impl<T: Copy> StagedScatter<T> {
    pub fn new(n: usize) -> Self {
        assert!((GROUP as usize * std::mem::size_of::<T>()) % 64 == 0, "groups must fill whole cache lines");
        StagedScatter {
            arena: Arena::new(),
            cursors: vec![Cursor { cur: std::ptr::null_mut(), fill: BLOCK, staged: 0 }; n],
            stage: (0..n * GROUP as usize).map(|_| unsafe { std::mem::zeroed() }).collect(),
            blocks: vec![Vec::new(); n],
        }
    }
    #[inline(never)]
    fn grow(&mut self, i: usize) {
        let p = self.arena.alloc(std::mem::size_of::<T>() * BLOCK as usize) as *mut T;
        self.cursors[i] = Cursor { cur: p, fill: 0, staged: 0 };
        self.blocks[i].push(p);
    }
    #[inline(always)]
    unsafe fn copy_group(dst: *mut T, src: *const T, bytes: usize) {
        #[cfg(target_arch = "x86_64")]
        {
            use std::arch::x86_64::*;
            let d = dst as *mut __m128i;
            let s = src as *const __m128i;
            for k in 0..bytes / 16 {
                _mm_stream_si128(d.add(k), _mm_loadu_si128(s.add(k)));
            }
        }
        #[cfg(not(target_arch = "x86_64"))]
        std::ptr::copy_nonoverlapping(src as *const u8, dst as *mut u8, bytes);
    }
    // i < the bucket count: callers take it from the top bits of a hash
    #[inline(always)]
    pub fn push(&mut self, i: usize, x: T) -> *mut T {
        if unsafe { self.cursors.get_unchecked(i) }.fill == BLOCK {
            self.grow(i);
        }
        let c = unsafe { self.cursors.get_unchecked_mut(i) };
        unsafe {
            let g = self.stage.as_mut_ptr().add(i * GROUP as usize);
            g.add(c.staged as usize).write(x);
            let place = c.cur.add((c.fill + c.staged) as usize);
            c.staged += 1;
            if c.staged == GROUP {
                Self::copy_group(c.cur.add(c.fill as usize), g, std::mem::size_of::<T>() * GROUP as usize);
                c.fill += GROUP;
                c.staged = 0;
            }
            place
        }
    }
    pub fn flush(&mut self) {
        for i in 0..self.cursors.len() {
            let c = &mut self.cursors[i];
            if c.staged != 0 {
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        self.stage.as_ptr().add(i * GROUP as usize),
                        c.cur.add(c.fill as usize),
                        c.staged as usize,
                    );
                }
                c.fill += c.staged;
                c.staged = 0;
            }
        }
        store_fence(); // the streamed lines are visible before anyone reads them
    }
    pub fn size(&self, i: usize) -> usize {
        let n = self.blocks[i].len();
        if n == 0 {
            0
        } else {
            (n - 1) * BLOCK as usize + self.cursors[i].fill as usize
        }
    }
    pub fn num_blocks(&self, i: usize) -> usize {
        self.blocks[i].len()
    }
    pub fn block(&self, i: usize, j: usize) -> (*const T, u32) {
        let b = &self.blocks[i];
        (b[j], if j + 1 == b.len() { self.cursors[i].fill } else { BLOCK })
    }
    #[inline(always)]
    pub fn for_each(&self, i: usize, mut f: impl FnMut(&T)) {
        let b = &self.blocks[i];
        for (j, &p) in b.iter().enumerate() {
            let n = if j + 1 == b.len() { self.cursors[i].fill } else { BLOCK };
            for k in 0..n as usize {
                f(unsafe { &*p.add(k) });
            }
        }
    }
}
