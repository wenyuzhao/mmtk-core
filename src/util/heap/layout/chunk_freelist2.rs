use crate::util::Address;

use super::vm_layout_constants::LOG_BYTES_IN_CHUNK;

#[derive(Clone)]
#[repr(transparent)]
struct CellMeta(u8);

impl CellMeta {
    const fn new() -> Self {
        Self(0)
    }

    fn is_free(&self) -> bool {
        self.0 & 1 != 0
    }

    fn set_free(&mut self, free: bool) {
        self.0 = if free {
            self.0 | 1
        } else {
            self.0 & !1u8
        };
    }

    fn size_class(&self) -> usize {
        ((self.0 >> 1) & 0b1111111) as usize
    }

    fn set_size_class(&mut self, sc: usize) {
        debug_assert!(sc < 0b1111111);
        let mask = 0b1111111u8 << 1;
        self.0 = (self.0 & !mask) | ((sc as u8) << 1);
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

    const NEXT_OFFSET: usize = 0;
    const PREV_OFFSET: usize = 8;

    fn next(&self, unit: usize) -> Option<usize> {
        let v = unsafe { self.unit_to_address(unit).add(Self::NEXT_OFFSET).load::<usize>() };
        if v == usize::MAX {
            None
        } else {
            Some(v)
        }
    }

    fn prev(&self, unit: usize) -> Option<usize> {
        let v = unsafe { self.unit_to_address(unit).add(Self::PREV_OFFSET).load::<usize>() };
        if v == usize::MAX {
            None
        } else {
            Some(v)
        }
    }

    fn set_next(&self, unit: usize, next: Option<usize>) {
        let v = next.unwrap_or(usize::MAX);
        unsafe { self.unit_to_address(unit).add(Self::NEXT_OFFSET).store(v) }
    }

    fn set_prev(&self, unit: usize, prev: Option<usize>) {
        let v = prev.unwrap_or(usize::MAX);
        unsafe { self.unit_to_address(unit).add(Self::PREV_OFFSET).store(v) }
    }

    fn size_class(&self, units: usize) -> usize {
        units.trailing_zeros() as usize
    }

    fn size_class_to_units(&self, sc: usize) -> usize {
        1 << sc
    }

    fn pop_cell(&mut self, sc: usize) -> Option<usize> {
        let curr = self.heads[sc]?;
        debug_assert_ne!(self.next(curr), Some(curr));
        let next = self.next(curr);
        self.set_next(curr, None);
        if let Some(next) = next {
            self.set_prev(next, None);
        }
        self.meta[curr].set_free(false);
        self.meta[curr].set_size_class(sc);
        self.heads[sc] = next;
        debug_assert!(self.next(curr).is_none());
        debug_assert!(self.prev(curr).is_none());
        Some(curr)
    }

    fn push_cell(&mut self, unit: usize, sc: usize) {
        debug_assert!(self.next(unit).is_none());
        debug_assert!(self.prev(unit).is_none());
        let curr = self.heads[sc];
        if let Some(curr) = curr {
            self.set_prev(curr, Some(unit));
        }
        self.set_prev(unit,None);
        self.set_next(unit, curr);
        self.heads[sc] = Some(unit);
        self.meta[unit].set_free(true);
        debug_assert!(self.meta[unit].is_free());
        self.meta[unit].set_size_class(sc);
        debug_assert!(self.meta[unit].is_free());
        debug_assert_ne!(self.next(unit), Some(unit));
    }

    fn remove_cell(&mut self, unit: usize, sc: usize) {
        let prev = self.prev(unit);
        let next = self.next(unit);
        if let Some(prev) = prev {
            self.set_next(prev, next);
        }
        if let Some(next) = next {
            self.set_prev(next, prev);
        }
        if self.heads[sc] == Some(unit) {
            self.heads[sc] = next;
        }
        self.set_next(unit, None);
        self.set_prev(unit, None);
        self.meta[unit].set_free(false);
        self.meta[unit].set_size_class(sc);
        debug_assert!(self.next(unit).is_none());
        debug_assert!(self.prev(unit).is_none());
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
        debug_assert!(unit < usize::MAX);
        self.base + (unit << LOG_BYTES_IN_CHUNK)
    }

    pub fn address_to_unit(&self, a: Address) -> usize {
        debug_assert!(a >= self.base);
        let unit = (a - self.base) >> LOG_BYTES_IN_CHUNK;
        debug_assert!(unit < usize::MAX);
        unit
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
            self.set_next(u, None);
            self.set_prev(u, None);
        }
        for u in unit..unit + units {
            self.free(u);
        }
    }
}
