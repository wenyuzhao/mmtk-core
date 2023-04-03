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
        debug_assert!(sc <= 0b1111111);
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
            debug_assert!(x != 0)
        }
        let index = index.unwrap_or(0);
        debug_assert!(index <= 0xfffffff);
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
            debug_assert!(x != 0)
        }
        let index = index.unwrap_or(0);
        debug_assert!(index <= 0xfffffff);
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
        debug_assert_ne!(self.meta[curr].next(), Some(curr));
        let next = self.meta[curr].next();
        self.meta[curr].set_next(None);
        if let Some(next) = next {
            self.meta[next].set_prev(None);
        }
        self.meta[curr].set_free(false);
        self.meta[curr].set_size_class(sc);
        self.heads[sc] = next;
        debug_assert!(self.meta[curr].next().is_none());
        debug_assert!(self.meta[curr].prev().is_none());
        Some(curr)
    }

    fn push_cell(&mut self, unit: usize, sc: usize) {
        debug_assert!(self.meta[unit].next().is_none());
        debug_assert!(self.meta[unit].prev().is_none());
        let curr = self.heads[sc];
        if let Some(curr) = curr {
            self.meta[curr].set_prev(Some(unit));
        }
        self.meta[unit].set_prev(None);
        self.meta[unit].set_next(curr);
        self.heads[sc] = Some(unit);
        self.meta[unit].set_free(true);
        debug_assert!(self.meta[unit].is_free());
        self.meta[unit].set_size_class(sc);
        debug_assert!(self.meta[unit].is_free());
        debug_assert_ne!(self.meta[unit].next(), Some(unit));
    }

    fn remove_cell(&mut self, unit: usize, sc: usize) {
        let prev = self.meta[unit].prev();
        let next = self.meta[unit].next();
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
        debug_assert!(self.meta[unit].next().is_none());
        debug_assert!(self.meta[unit].prev().is_none());
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
            let b = parent + self.size_class_to_units(sc);
            self.meta[a].set_size_class(sc);
            self.push_cell(b, sc);
            Some(a)
        }
    }

    pub fn free(&mut self, unit: usize) -> (usize, usize) {
        let sc = self.meta[unit].size_class();
        let sibling = unit ^ (1 << sc);
        if sc + 1 <= self.heads.len()
            && sibling > 0
            && sibling < self.meta.len()
            && self.meta[sibling].size_class() == sc
            && self.meta[sibling].is_free()
        {
            // coalesce
            self.remove_cell(sibling, sc);
            debug_assert!(!self.meta[sibling].is_free());
            debug_assert!(!self.meta[unit].is_free());
            let parent = usize::min(unit, sibling);
            self.meta[parent].set_size_class(sc + 1);
            let (_, coalesced_units) = self.free(parent);
            return (1 << sc, coalesced_units);
        } else {
            self.push_cell(unit, sc);
            debug_assert!(self.meta[unit].is_free());
            debug_assert!(self.meta[unit].size_class() == sc);
            return (1 << sc, 1 << sc);
        }
    }

    pub fn unit_to_address(&self, unit: usize) -> Address {
        debug_assert!(unit != 0);
        Address::ZERO + (unit << LOG_BYTES_IN_CHUNK)
        // self.base + ((unit - 1) << LOG_BYTES_IN_CHUNK)
    }

    pub fn address_to_unit(&self, a: Address) -> usize {
        debug_assert!(!a.is_zero());
        a.as_usize() >> LOG_BYTES_IN_CHUNK
        // ((a - self.base) >> LOG_BYTES_IN_CHUNK) + 1
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
        for u in unit..unit + units {
            self.free(u);
        }
    }
}
