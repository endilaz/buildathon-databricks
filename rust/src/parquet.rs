// Minimal Parquet writer: one row group, one uncompressed PLAIN data page
// (v1) per column, no statistics. Metadata in the Thrift compact protocol.
use crate::base::*;
use std::io::{IoSlice, Write};

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    String,
    OptString,
    Date,
    Int64,
}

pub struct Column {
    pub name: &'static str,
    pub kind: Kind,
    pub values: Vec<u8>,  // the PLAIN-encoded non-null values
    pub present: Vec<u8>, // OptString only: 1 per row when not null
}

impl Column {
    pub fn new(name: &'static str, kind: Kind) -> Column {
        Column { name, kind, values: Vec::new(), present: Vec::new() }
    }
    #[inline(always)]
    pub fn add_string(&mut self, s: &[u8]) {
        self.values.extend_from_slice(&(s.len() as u32).to_le_bytes());
        self.values.extend_from_slice(s);
    }
    #[inline(always)]
    pub fn add_key(&mut self, k: Key) {
        if is_long(k) {
            self.add_string(long_bytes(k));
            return;
        }
        let mut buf = [0u8; 16];
        let n = packed_bytes(k, &mut buf);
        self.add_string(&buf[..n]);
    }
    #[inline(always)]
    pub fn add_i32(&mut self, v: i32) {
        self.values.extend_from_slice(&v.to_le_bytes());
    }
    #[inline(always)]
    pub fn add_i64(&mut self, v: i64) {
        self.values.extend_from_slice(&v.to_le_bytes());
    }
}

struct Thrift {
    last: Vec<i16>,
    out: Vec<u8>,
}
impl Thrift {
    fn new() -> Thrift {
        Thrift { last: vec![0], out: Vec::new() }
    }
    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.out.push((v | 0x80) as u8);
            v >>= 7;
        }
        self.out.push(v as u8);
    }
    fn zigzag(&mut self, v: i64) {
        self.varint(((v as u64) << 1) ^ ((v >> 63) as u64));
    }
    fn field(&mut self, id: i16, ty: u8) {
        let last = self.last.last_mut().unwrap();
        let delta = id - *last;
        *last = id;
        if delta > 0 && delta <= 15 {
            self.out.push(((delta as u8) << 4) | ty);
        } else {
            self.out.push(ty);
            self.zigzag(id as i64);
        }
    }
    fn i32_field(&mut self, id: i16, v: i64) {
        self.field(id, 5);
        self.zigzag(v);
    }
    fn i64_field(&mut self, id: i16, v: i64) {
        self.field(id, 6);
        self.zigzag(v);
    }
    fn string_field(&mut self, id: i16, s: &str) {
        self.field(id, 8);
        self.varint(s.len() as u64);
        self.out.extend_from_slice(s.as_bytes());
    }
    fn list_field(&mut self, id: i16, elem: u8, n: usize) {
        self.field(id, 9);
        if n < 15 {
            self.out.push(((n as u8) << 4) | elem);
        } else {
            self.out.push(0xF0 | elem);
            self.varint(n as u64);
        }
    }
    fn begin_struct(&mut self, id: i16) {
        self.field(id, 12);
        self.last.push(0);
    }
    fn begin_element(&mut self) {
        self.last.push(0);
    }
    fn end_struct(&mut self) {
        self.out.push(0);
        self.last.pop();
    }
    fn stop(&mut self) {
        self.out.push(0);
    }
}

const STRUCT: u8 = 12;
const I32: u8 = 5;
const BINARY: u8 = 8;

fn physical(k: Kind) -> i64 {
    match k {
        Kind::Int64 => 2,
        Kind::Date => 1,
        _ => 6,
    }
}

// A piece of the file: an owned header or a column's own value buffer, so
// no column is copied before the write.
enum Piece {
    Owned(Vec<u8>),
    Values(usize),
}

pub fn write_file(path: &str, cols: &[Column], rows: usize) {
    let mut pieces: Vec<Piece> = vec![Piece::Owned(b"PAR1".to_vec())];
    let mut size = 4usize;
    let mut chunks: Vec<(i64, i64)> = Vec::new(); // offset, size
    if rows > 0 {
        for (ci, c) in cols.iter().enumerate() {
            let mut levels = Vec::new();
            if c.kind == Kind::OptString {
                // definition levels: one bit-packed run of width 1, length-prefixed
                let groups = rows.div_ceil(8);
                let mut h = Thrift::new();
                h.varint(((groups as u64) << 1) | 1);
                let n = (h.out.len() + groups) as u32;
                levels.extend_from_slice(&n.to_le_bytes());
                levels.extend_from_slice(&h.out);
                let base = levels.len();
                levels.resize(base + groups, 0);
                for i in 0..rows {
                    levels[base + i / 8] |= c.present[i] << (i % 8);
                }
            }
            let body = (levels.len() + c.values.len()) as i64;
            let mut ph = Thrift::new();
            ph.i32_field(1, 0); // DATA_PAGE
            ph.i32_field(2, body);
            ph.i32_field(3, body);
            ph.begin_struct(5);
            ph.i32_field(1, rows as i64);
            ph.i32_field(2, 0); // PLAIN
            ph.i32_field(3, 3); // RLE
            ph.i32_field(4, 3); // RLE
            ph.end_struct();
            ph.stop();
            let offset = size as i64;
            size += ph.out.len();
            pieces.push(Piece::Owned(ph.out));
            if !levels.is_empty() {
                size += levels.len();
                pieces.push(Piece::Owned(levels));
            }
            size += c.values.len();
            pieces.push(Piece::Values(ci));
            chunks.push((offset, size as i64 - offset));
        }
    }
    let mut t = Thrift::new();
    t.i32_field(1, 1);
    t.list_field(2, STRUCT, cols.len() + 1);
    t.begin_element();
    t.string_field(4, "schema");
    t.i32_field(5, cols.len() as i64);
    t.end_struct();
    for c in cols {
        t.begin_element();
        t.i32_field(1, physical(c.kind));
        t.i32_field(3, if c.kind == Kind::OptString { 1 } else { 0 }); // OPTIONAL : REQUIRED
        t.string_field(4, c.name);
        if c.kind == Kind::String || c.kind == Kind::OptString {
            t.i32_field(6, 0); // converted UTF8
            t.begin_struct(10);
            t.begin_struct(1); // LogicalType.STRING
            t.end_struct();
            t.end_struct();
        } else if c.kind == Kind::Date {
            t.i32_field(6, 6); // converted DATE
            t.begin_struct(10);
            t.begin_struct(6); // LogicalType.DATE
            t.end_struct();
            t.end_struct();
        }
        t.end_struct();
    }
    t.i64_field(3, rows as i64);
    if rows > 0 {
        t.list_field(4, STRUCT, 1);
        t.begin_element();
        t.list_field(1, STRUCT, cols.len());
        let mut total = 0i64;
        for (i, c) in cols.iter().enumerate() {
            let (offset, csize) = chunks[i];
            total += csize;
            t.begin_element();
            t.i64_field(2, offset);
            t.begin_struct(3);
            t.i32_field(1, physical(c.kind));
            t.list_field(2, I32, 2);
            t.zigzag(0); // PLAIN
            t.zigzag(3); // RLE
            t.list_field(3, BINARY, 1);
            t.varint(c.name.len() as u64);
            t.out.extend_from_slice(c.name.as_bytes());
            t.i32_field(4, 0); // UNCOMPRESSED
            t.i64_field(5, rows as i64);
            t.i64_field(6, csize);
            t.i64_field(7, csize);
            t.i64_field(9, offset);
            t.end_struct();
            t.end_struct();
        }
        t.i64_field(2, total);
        t.i64_field(3, rows as i64);
        t.end_struct();
    } else {
        t.list_field(4, STRUCT, 0);
    }
    t.string_field(6, "shopping-sessions");
    t.stop();
    let n = t.out.len() as u32;
    t.out.extend_from_slice(&n.to_le_bytes());
    t.out.extend_from_slice(b"PAR1");
    pieces.push(Piece::Owned(t.out));

    let mut slices: Vec<IoSlice> = pieces
        .iter()
        .map(|p| match p {
            Piece::Owned(v) => IoSlice::new(v),
            Piece::Values(i) => IoSlice::new(&cols[*i].values),
        })
        .filter(|s| !s.is_empty())
        .collect();
    let mut f = std::fs::File::create(path).unwrap_or_else(|_| die(&format!("cannot create {path}")));
    let mut rest: &mut [IoSlice] = &mut slices;
    while !rest.is_empty() {
        match f.write_vectored(rest) {
            Ok(w) if w > 0 => IoSlice::advance_slices(&mut rest, w),
            _ => die(&format!("cannot write {path}")),
        }
    }
}
