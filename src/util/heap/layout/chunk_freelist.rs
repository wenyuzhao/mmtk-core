use crate::util::Address;

use super::vm_layout_constants::LOG_BYTES_IN_CHUNK;

#[derive(Clone)]
#[repr(transparent)]
struct CellMeta(u64);

impl CellMeta {
    const fn new() -> Self {
        Self(0)
    }

    fn is_free(&self) -> bool {
        self.0 & (1 << 63) != 0
    }

    fn set_free(&mut self, free: bool) {
        self.0 = if free {
            self.0 | (1 << 63)
        } else {
            self.0 << 1 >> 1
        };
    }

    fn size_class(&self) -> usize {
        ((self.0 >> 56) & 0b1111111) as usize
    }

    fn set_size_class(&mut self, sc: usize) {
        assert!(sc <= 0b1111111);
        let mask = 0b1111111u64 << 56;
        self.0 = (self.0 & !mask) | ((sc as u64) << 56);
    }

    fn next(&self) -> Option<usize> {
        let v = ((self.0 >> 28) & 0xfffffffu64) as usize;
        if v == 0 {
            None
        } else {
            Some(v)
        }
    }

    fn set_next(&mut self, index: Option<usize>) {
        if let Some(x) = index {
            assert!(x != 0)
        }
        let index = index.unwrap_or(0);
        assert!(index <= 0xfffffff);
        let mask = 0xfffffffu64 << 28;
        self.0 = (self.0 & !mask) | ((index as u64) << 28);
    }

    fn prev(&self) -> Option<usize> {
        let v = ((self.0 >> 0) & 0xfffffffu64) as usize;
        if v == 0 {
            None
        } else {
            Some(v)
        }
    }

    fn set_prev(&mut self, index: Option<usize>) {
        if let Some(x) = index {
            assert!(x != 0)
        }
        let index = index.unwrap_or(0);
        assert!(index <= 0xfffffff);
        let mask = 0xfffffffu64 << 0;
        self.0 = (self.0 & !mask) | ((index as u64) << 0);
    }
}

pub struct ChunkFreelist {
    base: Address,
    heads: [Option<usize>; 26],
    meta: Vec<CellMeta>,
}

impl ChunkFreelist {
    pub fn new(base: Address) -> Self {
        Self {
            base,
            heads: [None; 26],
            meta: vec![],
        }
    }

    fn size_class(&self, units: usize) -> usize {
        units.trailing_zeros() as usize
    }

    fn size_class_to_units(&self, sc: usize) -> usize {
        1 << sc
    }

    fn pop_cell(&mut self, sc: usize) -> Option<usize> {
        let curr = self.heads[sc]?;
        let next = self.meta[curr].next();
        self.meta[curr].set_next(None);
        if let Some(next) = next {
            self.meta[next].set_prev(None);
        }
        self.meta[curr].set_free(false);
        self.meta[curr].set_size_class(sc);
        self.heads[sc] = next;
        println!(
            "pop cell {} {:?} {:?} next={:?}",
            sc,
            curr,
            self.unit_to_address(curr),
            next
        );
        Some(curr)
    }

    fn push_cell(&mut self, unit: usize, sc: usize) {
        let curr = self.heads[sc];
        if let Some(curr) = curr {
            self.meta[curr].set_prev(Some(unit));
        }
        self.meta[unit].set_prev(None);
        self.meta[unit].set_next(curr);
        self.heads[sc] = Some(unit);
        println!(
            "push cell {} {:?} {:?} next={:?}",
            sc,
            unit,
            self.unit_to_address(unit),
            curr
        );
        // println!("push cell {} {:?}", sc, self.heads[sc]);
        self.meta[unit].set_free(true);
        assert!(self.meta[unit].is_free());
        self.meta[unit].set_size_class(sc);
        assert!(self.meta[unit].is_free());
    }

    fn remove_cell(&mut self, unit: usize, sc: usize) {
        let prev = self.meta[unit].prev();
        let next = self.meta[unit].prev();
        if let Some(prev) = prev {
            self.meta[prev].set_next(next);
        }
        if let Some(next) = next {
            self.meta[next].set_prev(prev);
        }
        if self.heads[sc] == Some(unit) {
            self.heads[sc] = next;
        }
        self.meta[unit].set_next(None);
        self.meta[unit].set_prev(None);
        self.meta[unit].set_free(false);
        self.meta[unit].set_size_class(sc);
    }

    pub fn alloc(&mut self, units: usize) -> Option<usize> {
        let aligned_units = units.next_power_of_two();
        let sc = self.size_class(aligned_units);
        if let Some(cell) = self.pop_cell(sc) {
            Some(cell)
        } else {
            let next_sc = sc + 1;
            if next_sc >= self.heads.len() {
                return None;
            }
            // alloc and split a larger cell
            let parent = self.alloc(self.size_class_to_units(next_sc))?;
            let a = parent;
            self.meta[a].set_size_class(sc);
            let b = parent + self.size_class_to_units(sc);
            self.push_cell(b, sc);
            Some(a)
        }
    }

    pub fn free(&mut self, unit: usize) -> (usize, usize) {
        let sc = self.meta[unit].size_class();
        let sibling = unit ^ (1 << sc);
        println!(
            "free sc={} u={} s={} {} {} ",
            sc,
            unit,
            sibling,
            self.meta[sibling].size_class(),
            self.meta[sibling].is_free()
        );
        if sc + 1 <= self.heads.len()
            && sibling > 0
            && sibling < self.meta.len()
            && self.meta[sibling].size_class() == sc
            && self.meta[sibling].is_free()
        {
            // coalesce
            self.remove_cell(sibling, sc);
            let parent = usize::min(unit, sibling);
            self.meta[parent].set_size_class(sc + 1);
            let (_, coalesced_units) = self.free(parent);
            assert!(self.meta[parent].size_class() == sc + 1);
            assert!(self.meta[parent].is_free(), "parent {} is not free", parent);
            assert!(!self.meta[if parent == sibling { unit } else { parent }].is_free());

            println!("free coalesced: {:?} {} {}", unit, 1 << sc, coalesced_units);
            return (1 << sc, coalesced_units);
        } else {
            self.push_cell(unit, sc);
            assert!(self.meta[unit].is_free());
            assert!(self.meta[unit].size_class() == sc);
            println!(
                "free direct: {:?} {} {}",
                unit,
                1 << sc,
                self.meta[unit].is_free()
            );
            return (1 << sc, 1 << sc);
        }
    }

    pub fn unit_to_address(&self, unit: usize) -> Address {
        assert!(unit != 0);
        self.base + ((unit - 1) << LOG_BYTES_IN_CHUNK)
    }

    pub fn address_to_unit(&self, a: Address) -> usize {
        assert!(!a.is_zero());
        ((a - self.base) >> LOG_BYTES_IN_CHUNK) + 1
    }

    pub fn size(&self, unit: usize) -> usize {
        self.size_class_to_units(self.meta[unit].size_class())
    }

    pub fn insert(&mut self, unit: usize, units: usize) {
        let cap = unit + units.next_power_of_two();
        self.meta.resize(cap, CellMeta(0));
        let sc = self.size_class(1);
        for u in unit..unit + units {
            self.meta[u].set_free(false);
            self.meta[u].set_size_class(sc);
        }
        let mut y = 0;
        for u in unit..unit + units {
            let x = self.free(u);
            println!("{}: {:?}", u, x);
            y = usize::max(x.1, y);
        }
        assert!(y > 1);
    }
}
