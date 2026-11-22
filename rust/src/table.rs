// Linear-probing map from Key (never 0) to a small value, key and value in
// one slot so a hit touches one cache line.
use crate::base::*;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct KeySlot<V: Copy> {
    pub k: Key,
    pub v: V,
}

pub struct KeyMap<V: Copy> {
    slots: Vec<KeySlot<V>>,
    mask: usize,
    size: usize,
}

fn capacity_for(n: usize) -> usize {
    let mut cap = 16;
    while cap < n * 2 {
        cap <<= 1;
    }
    cap
}

impl<V: Copy> KeyMap<V> {
    pub fn new(n: usize) -> Self {
        let mut m = KeyMap { slots: Vec::new(), mask: 0, size: 0 };
        m.reset(n);
        m
    }
    fn zeroed(n: usize) -> Vec<KeySlot<V>> {
        // all-zero slots are empty; V is a plain integer
        let mut v = Vec::with_capacity(n);
        unsafe {
            std::ptr::write_bytes(v.as_mut_ptr(), 0, n);
            v.set_len(n);
        }
        v
    }
    pub fn reset(&mut self, n: usize) {
        let cap = capacity_for(n);
        if cap == self.slots.len() {
            unsafe { std::ptr::write_bytes(self.slots.as_mut_ptr(), 0, cap) };
        } else {
            self.slots = Self::zeroed(cap);
        }
        self.mask = cap - 1;
        self.size = 0;
    }
    fn grow(&mut self) {
        let cap = capacity_for(self.slots.len());
        let old = std::mem::replace(&mut self.slots, Self::zeroed(cap));
        self.mask = self.slots.len() - 1;
        self.size = 0;
        for s in old {
            if s.k != 0 {
                self.insert(s.k, s.v);
            }
        }
    }
    #[inline(always)]
    pub fn find(&self, k: Key) -> Option<V> {
        let mut i = key_hash(k) as usize & self.mask;
        loop {
            let s = unsafe { self.slots.get_unchecked(i) };
            if s.k == k {
                return Some(s.v);
            }
            if s.k == 0 {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }
    // Inserts make() if k is absent; returns k's value.
    #[inline(always)]
    pub fn get_or_insert(&mut self, k: Key, make: impl FnOnce() -> V) -> V {
        if (self.size + 1) * 4 > self.slots.len() * 3 {
            self.grow();
        }
        let mut i = key_hash(k) as usize & self.mask;
        loop {
            let s = unsafe { self.slots.get_unchecked_mut(i) };
            if s.k == k {
                return s.v;
            }
            if s.k == 0 {
                s.k = k;
                s.v = make();
                self.size += 1;
                return s.v;
            }
            i = (i + 1) & self.mask;
        }
    }
    pub fn insert(&mut self, k: Key, v: V) {
        if (self.size + 1) * 4 > self.slots.len() * 3 {
            self.grow();
        }
        let mut i = key_hash(k) as usize & self.mask;
        loop {
            let s = &mut self.slots[i];
            if s.k == k || s.k == 0 {
                if s.k == 0 {
                    self.size += 1;
                }
                s.k = k;
                s.v = v;
                return;
            }
            i = (i + 1) & self.mask;
        }
    }
}
