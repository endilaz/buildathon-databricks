// CSV line splitting with 64-byte separator bitmasks, timestamp and price
// parsing. Inputs carry at least 64 readable bytes past their end.
use crate::base::*;

// ---------------------------------------------------------------- scanner

// bit i: byte i is '\n'; ',' or '\n'; '"'
#[derive(Clone, Copy)]
struct Masks {
    nl: u64,
    sep: u64,
    quote: u64,
}

#[inline(always)]
unsafe fn block_masks(p: *const u8) -> Masks {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        use std::arch::x86_64::*;
        let a = _mm256_loadu_si256(p as *const __m256i);
        let b = _mm256_loadu_si256(p.add(32) as *const __m256i);
        let eq = |c: u8| {
            let v = _mm256_set1_epi8(c as i8);
            let lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(a, v)) as u32 as u64;
            let hi = _mm256_movemask_epi8(_mm256_cmpeq_epi8(b, v)) as u32 as u64;
            lo | (hi << 32)
        };
        let nl = eq(b'\n');
        Masks { nl, sep: nl | eq(b','), quote: eq(b'"') }
    }
    #[cfg(target_arch = "aarch64")]
    {
        use std::arch::aarch64::*;
        let v0 = vld1q_u8(p);
        let v1 = vld1q_u8(p.add(16));
        let v2 = vld1q_u8(p.add(32));
        let v3 = vld1q_u8(p.add(48));
        const WEIGHTS: [u8; 16] = [1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128];
        let w = vld1q_u8(WEIGHTS.as_ptr());
        let eq = |c: u8| {
            let s = vdupq_n_u8(c);
            let a = vpaddq_u8(vandq_u8(vceqq_u8(v0, s), w), vandq_u8(vceqq_u8(v1, s), w));
            let b = vpaddq_u8(vandq_u8(vceqq_u8(v2, s), w), vandq_u8(vceqq_u8(v3, s), w));
            let a = vpaddq_u8(a, b);
            let a = vpaddq_u8(a, a);
            vgetq_lane_u64(vreinterpretq_u64_u8(a), 0)
        };
        let nl = eq(b'\n');
        Masks { nl, sep: nl | eq(b','), quote: eq(b'"') }
    }
    #[cfg(not(any(all(target_arch = "x86_64", target_feature = "avx2"), target_arch = "aarch64")))]
    {
        let mut m = Masks { nl: 0, sep: 0, quote: 0 };
        for i in 0..64 {
            let bit = 1u64 << i;
            match *p.add(i) {
                b'\n' => m.nl |= bit,
                b',' => m.sep |= bit,
                b'"' => m.quote |= bit,
                _ => {}
            }
        }
        m.sep |= m.nl;
        m
    }
}

// Walks data[0, len) from a line start. The end of the data acts as a
// newline. A field is well formed when it has no quotes, or is wrapped in
// one pair with none inside; a per-line quote count checks the latter.
struct Scanner {
    d: *const u8,
    len: usize,
    base: usize,
    seps: u64,
    nl: u64,
    quote: u64,
    quotes: u32,      // quotes in [start, base)
    line_quotes: u32, // quotes in [start, current line start)
}

impl Scanner {
    #[inline(always)]
    fn load(&mut self) {
        let mut m = unsafe { block_masks(self.d.add(self.base)) };
        let left = self.len - self.base;
        if left < 64 {
            let keep = (1u64 << left) - 1;
            m.nl = (m.nl & keep) | (1u64 << left);
            m.sep = (m.sep & keep) | m.nl;
            m.quote &= keep;
        }
        self.nl = m.nl;
        self.quote = m.quote;
        self.seps = m.sep;
    }
    fn new(d: *const u8, len: usize, pos: usize) -> Scanner {
        let mut s = Scanner { d, len, base: pos, seps: 0, nl: 0, quote: 0, quotes: 0, line_quotes: 0 };
        s.load();
        s
    }

    // Splits the line at p (where the previous line ended) into N fields.
    // Returns the next line's start and whether the line is well formed.
    #[inline(always)]
    fn line<const N: usize>(&mut self, mut p: usize, f: &mut [Field; N]) -> (usize, bool) {
        let mut k = 0usize;
        let mut expect = 0u32;
        loop {
            while self.seps == 0 {
                self.quotes += self.quote.count_ones();
                self.base += 64;
                self.load();
            }
            let bit = self.seps.trailing_zeros();
            self.seps &= self.seps - 1;
            let q = self.base + bit as usize;
            let a = unsafe { self.d.add(p) };
            let n = q - p;
            // branch-free: fields are quoted at random, so a branch here mispredicts
            // (for n == 0 the last byte read is the field's own separator)
            let quoted = unsafe {
                (n >= 2) as u32 & (*a == b'"') as u32 & (*a.add(n - (n != 0) as usize) == b'"') as u32
            };
            expect += 2 * quoted;
            if k < N {
                f[k] = Field { p: unsafe { a.add(quoted as usize) }, n: n - 2 * quoted as usize };
            }
            if (self.nl >> bit) & 1 != 0 {
                let seen = self.quotes + (self.quote & ((1u64 << bit) - 1)).count_ones();
                let ok = k == N - 1 && seen - self.line_quotes == expect;
                self.line_quotes = seen;
                return (q + 1, ok);
            }
            k += 1;
            p = q + 1;
        }
    }
}

// Calls f(fields) for each well-formed line starting in [s, e). A file's
// first line is its header. data has 64 readable bytes past len.
#[inline(always)]
pub fn for_lines<const N: usize>(d: *const u8, len: usize, s: usize, e: usize, mut f: impl FnMut(&[Field; N])) {
    let mut p = if s == 0 || unsafe { *d.add(s - 1) } != b'\n' {
        let rest = unsafe { std::slice::from_raw_parts(d.add(s), len - s) };
        match rest.iter().position(|&b| b == b'\n') {
            Some(i) => s + i + 1,
            None => len,
        }
    } else {
        s
    };
    if p >= e || p >= len {
        return;
    }
    let mut sc = Scanner::new(d, len, p);
    let mut fields = [Field::EMPTY; N];
    while p < e && p < len {
        let (next, ok) = sc.line::<N>(p, &mut fields);
        p = next;
        if ok {
            f(&fields);
        }
    }
}

// ---------------------------------------------------------------- time

pub const SECOND: i64 = 1000;
pub const MINUTE: i64 = 60 * SECOND;
pub const HOUR: i64 = 60 * MINUTE;
pub const DAY: i64 = 24 * HOUR;

pub const fn floor_div(a: i64, b: i64) -> i64 {
    a / b - ((a % b != 0) & ((a < 0) != (b < 0))) as i64
}

pub const fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = y - (m <= 2) as i64;
    let era = floor_div(y, 400);
    let yoe = y - era * 400;
    let doy = (153 * (m + if m > 2 { -3 } else { 9 }) + 2) / 5 + d - 1;
    era * 146097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719468
}

const fn is_leap(y: u32) -> bool {
    y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)
}
const MONTH_DAYS: [u8; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

#[inline(always)]
fn dig(c: u8) -> u32 {
    (c as u32).wrapping_sub(b'0' as u32)
}

const ASCII_ZEROS: u64 = 0x3030303030303030;
const HIGH_BITS: u64 = 0x8080808080808080;

// The bytes of x (input already XORed with '0's) under mask's high bits are
// all 0..9. A carry out of one byte only arises from a byte >= 0x8A, which is
// itself flagged, so the verdict is exact.
#[inline(always)]
fn digits_ok(x: u64, mask: u64) -> bool {
    ((x | x.wrapping_add(0x7676767676767676)) & mask) == 0
}
#[inline(always)]
fn byte_at(x: u64, i: u32) -> u32 {
    ((x >> (8 * i)) & 0xFF) as u32
}

// For each month of 1970..2099: epoch day of its 1st, and its length.
const TABLE_YEAR0: u32 = 1970;
const TABLE_YEARS: u32 = 130;
const fn make_month_table() -> ([i32; 1560], [u8; 1560]) {
    let mut first = [0i32; 1560];
    let mut days = [0u8; 1560];
    let mut i = 0;
    while i < 1560 {
        let y = TABLE_YEAR0 + i as u32 / 12;
        let m = i as u32 % 12 + 1;
        first[i] = days_from_civil(y as i64, m as i64, 1) as i32;
        days[i] = MONTH_DAYS[(m - 1) as usize] + (m == 2 && is_leap(y)) as u8;
        i += 1;
    }
    (first, days)
}
static MONTHS: ([i32; 1560], [u8; 1560]) = make_month_table();

// YYYY-MM-DDTHH:MM:SS at p, in epoch milliseconds.
#[inline(always)]
unsafe fn parse_civil(p: *const u8) -> Option<i64> {
    // little-endian loads: byte i of a word is p[offset + i]
    let a = load64(p); // Y Y Y Y - M M -
    let b = load64(p.add(8)); // D D T H H : M M
    let c = load64(p.add(16)) & 0xFFFFFF; // : S S
    let seps = (a & 0xFF0000FF00000000) == 0x2D00002D00000000 // '-' at 4 and 7
        && (b & 0x0000FF0000FF0000) == 0x00003A0000540000 // 'T' at 2, ':' at 5
        && (c & 0xFF) == 0x3A; // ':'
    let xa = a ^ ASCII_ZEROS;
    let xb = b ^ ASCII_ZEROS;
    let xc = c ^ 0x303030;
    if !seps || !digits_ok(xa, 0x0080800080808080) || !digits_ok(xb, 0x8080008080008080) || !digits_ok(xc, 0x808000) {
        return None;
    }
    let year = byte_at(xa, 0) * 1000 + byte_at(xa, 1) * 100 + byte_at(xa, 2) * 10 + byte_at(xa, 3);
    let month = byte_at(xa, 5) * 10 + byte_at(xa, 6);
    let day = byte_at(xb, 0) * 10 + byte_at(xb, 1);
    let hour = byte_at(xb, 3) * 10 + byte_at(xb, 4);
    let minute = byte_at(xb, 6) * 10 + byte_at(xb, 7);
    let second = byte_at(xc, 1) * 10 + byte_at(xc, 2);
    if month.wrapping_sub(1) > 11 || day == 0 || hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let days;
    if year.wrapping_sub(TABLE_YEAR0) < TABLE_YEARS {
        let i = ((year - TABLE_YEAR0) * 12 + month - 1) as usize;
        if day > MONTHS.1[i] as u32 {
            return None;
        }
        days = MONTHS.0[i] as i64 + day as i64 - 1;
    } else {
        if day > MONTH_DAYS[(month - 1) as usize] as u32 + (month == 2 && is_leap(year)) as u32 {
            return None;
        }
        days = days_from_civil(year as i64, month as i64, day as i64);
    }
    Some(days * DAY + hour as i64 * HOUR + minute as i64 * MINUTE + second as i64 * SECOND)
}

// 8 digit values (one per byte, first digit lowest) -> their decimal value.
#[inline(always)]
fn eight_digits(x: u64) -> u64 {
    let x = x.wrapping_mul(10).wrapping_add(x >> 8); // byte 2i: d[2i] * 10 + d[2i + 1]
    ((x & 0x000000FF000000FF).wrapping_mul(100 + (1000000u64 << 32))
        .wrapping_add(((x >> 16) & 0x000000FF000000FF).wrapping_mul(1 + (10000u64 << 32))))
        >> 32
}

// <prefix><1..7 digits without a leading zero> -> the number.
#[inline(always)]
pub fn prefixed_number(id: Field, prefix: u8) -> Option<u32> {
    let n = id.len();
    unsafe {
        if !(2..=8).contains(&n) || id.at(0) != prefix || id.at(1) == b'0' {
            return None;
        }
        let m = n - 1;
        let x = load64(id.p.add(1)) ^ ASCII_ZEROS;
        if !digits_ok(x, HIGH_BITS >> (8 * (8 - m))) {
            return None;
        }
        // shift the digits to the top (zeros enter as leading digits; bytes past the field fall off)
        Some(eight_digits(x << (8 * (8 - m))) as u32)
    }
}

// Epoch milliseconds as ASCII digits (any length, leading zeros allowed).
// A value beyond i64 is reported as invalid: it is outside every window.
#[inline(always)]
unsafe fn parse_epoch_digits(f: Field) -> Option<i64> {
    let mut p = f.p;
    let mut n = f.n;
    if n == 0 {
        return None;
    }
    let mut i = 0;
    while i < n {
        let left = n - i;
        let mask = if left >= 8 { HIGH_BITS } else { HIGH_BITS >> (8 * (8 - left)) };
        if !digits_ok(load64(p.add(i)) ^ ASCII_ZEROS, mask) {
            return None;
        }
        i += 8;
    }
    while n > 19 && *p == b'0' {
        p = p.add(1);
        n -= 1;
    }
    if n > 19 {
        return None;
    }
    let mut v = 0u64;
    let head = n % 8;
    for j in 0..head {
        v = v * 10 + dig(*p.add(j)) as u64;
    }
    let mut j = head;
    while j < n {
        v = v.wrapping_mul(100000000).wrapping_add(eight_digits(load64(p.add(j)) ^ ASCII_ZEROS));
        j += 8;
    }
    // 19 digits fit in u64 without wrapping (< 10^19 < 2^64)
    if v > i64::MAX as u64 {
        return None;
    }
    Some(v as i64)
}

// Any of: YYYY-MM-DDTHH:MM:SSZ, YYYY-MM-DDTHH:MM:SS(+|-)HH:MM, epoch-ms digits.
#[inline(always)]
pub fn parse_ts(f: Field) -> Option<i64> {
    let n = f.len();
    unsafe {
        if (n == 20 || n == 25) && f.at(4) == b'-' {
            let t = parse_civil(f.p)?;
            if n == 20 {
                return if f.at(19) == b'Z' { Some(t) } else { None };
            }
            let sign = f.at(19);
            let (a, b, c, d) = (dig(f.at(20)), dig(f.at(21)), dig(f.at(23)), dig(f.at(24)));
            if (sign != b'+' && sign != b'-') || f.at(22) != b':' || a > 9 || b > 9 || c > 9 || d > 9 {
                return None;
            }
            let oh = a * 10 + b;
            let om = c * 10 + d;
            if oh > 23 || om > 59 {
                return None;
            }
            let off = oh as i64 * HOUR + om as i64 * MINUTE;
            return Some(if sign == b'+' { t - off } else { t + off });
        }
        parse_epoch_digits(f)
    }
}

#[inline(always)]
pub fn parse_f1(f: Field) -> Option<i64> {
    unsafe {
        if f.len() == 20 && f.at(19) == b'Z' {
            parse_civil(f.p)
        } else {
            None
        }
    }
}

// A purchase price: ASCII digits, value 1..10,000,000.
#[inline(always)]
pub fn parse_price(f: Field) -> Option<u32> {
    if f.is_empty() {
        return None;
    }
    let mut v = 0u32;
    for &c in f.bytes() {
        let d = dig(c);
        if d > 9 {
            return None;
        }
        v = v * 10 + d;
        if v > 10000000 {
            return None;
        }
    }
    if v == 0 {
        None
    } else {
        Some(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn field(s: &[u8], buf: &mut Vec<u8>) -> Field {
        buf.clear();
        buf.extend_from_slice(s);
        buf.extend(std::iter::repeat(b'#').take(64));
        Field { p: buf.as_ptr(), n: s.len() }
    }

    #[test]
    fn timestamps() {
        let mut b = Vec::new();
        assert_eq!(parse_ts(field(b"1970-01-02T00:00:00Z", &mut b)), Some(86_400_000));
        assert_eq!(parse_ts(field(b"2028-02-29T12:00:00+01:30", &mut b)),
                   Some(days_from_civil(2028, 2, 29) * DAY + 12 * HOUR - 90 * MINUTE));
        assert_eq!(parse_ts(field(b"2027-02-29T12:00:00Z", &mut b)), None);
        assert_eq!(parse_ts(field(b"2027-01-01T24:00:00Z", &mut b)), None);
        assert_eq!(parse_ts(field(b"2027-01-01T23:00:00+0200", &mut b)), None);
        assert_eq!(parse_ts(field(b"86400000", &mut b)), Some(86_400_000));
        assert_eq!(parse_ts(field(b"000000000000000000000086400000", &mut b)), Some(86_400_000));
        assert_eq!(parse_ts(field(b"99999999999999999999", &mut b)), None);
        assert_eq!(parse_ts(field(b"1.79e12", &mut b)), None);
        assert_eq!(parse_price(field(b"0250", &mut b)), Some(250));
        assert_eq!(parse_price(field(b"10000001", &mut b)), None);
        assert_eq!(prefixed_number(field(b"p15616", &mut b), b'p'), Some(15616));
        assert_eq!(prefixed_number(field(b"p0", &mut b), b'p'), None);
    }
}
