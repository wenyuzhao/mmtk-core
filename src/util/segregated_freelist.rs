use crate::util::{constants::LOG_BYTES_IN_PAGE, Address};

#[derive(Clone)]
#[repr(transparent)]
struct CellMeta2(u8);

impl CellMeta2 {
    const fn new() -> Self {
        Self(0)
    }

    fn is_free(&self) -> bool {
        self.0 & 1 != 0
    }

    fn set_free(&mut self, free: bool) {
        self.0 = if free { self.0 | 1 } else { self.0 & !1u8 };
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

#[derive(Clone)]
#[repr(transparent)]
struct CellMeta(u64);

impl CellMeta {
    const SENTINEL: usize = 0xfffffff;
    const UNIT_MASK: u64 = 0xfffffffu64;
    const NEXT_SHIFT: usize = 28;
    const PREV_SHIFT: usize = 0;

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
        debug_assert!(sc < 0b1111111);
        let mask = 0b1111111u64 << 56;
        self.0 = (self.0 & !mask) | ((sc as u64) << 56);
    }

    fn next(&self) -> Option<usize> {
        let v = ((self.0 >> Self::NEXT_SHIFT) & Self::UNIT_MASK) as usize;
        if v == Self::SENTINEL {
            None
        } else {
            Some(v)
        }
    }

    fn set_next(&mut self, index: Option<usize>) {
        if let Some(x) = index {
            debug_assert!(x < Self::SENTINEL)
        }
        let index = index.unwrap_or(Self::SENTINEL);
        debug_assert!(index <= Self::SENTINEL);
        let mask = Self::UNIT_MASK << Self::NEXT_SHIFT;
        self.0 = (self.0 & !mask) | ((index as u64) << Self::NEXT_SHIFT);
    }

    fn prev(&self) -> Option<usize> {
        let v = ((self.0 >> Self::PREV_SHIFT) & Self::UNIT_MASK) as usize;
        if v == Self::SENTINEL {
            None
        } else {
            Some(v)
        }
    }

    fn set_prev(&mut self, index: Option<usize>) {
        if let Some(x) = index {
            debug_assert!(x < Self::SENTINEL)
        }
        let index = index.unwrap_or(Self::SENTINEL);
        debug_assert!(index <= Self::SENTINEL);
        let mask = Self::UNIT_MASK << Self::PREV_SHIFT;
        self.0 = (self.0 & !mask) | ((index as u64) << Self::PREV_SHIFT);
    }
}

pub struct SegregatedFreelist {
    pub base: Address,
    heads: [Option<usize>; 26],
    meta: Vec<CellMeta>,
    units_on_freelist: usize,
}

const SIDE_FREELIST: bool = true;
const SENTINEL: usize = CellMeta::SENTINEL;

impl SegregatedFreelist {
    pub fn new(base: Address) -> Self {
        Self {
            base,
            heads: [None; 26],
            meta: vec![],
            units_on_freelist: 0,
        }
    }

    const NEXT_OFFSET: usize = 0;
    const PREV_OFFSET: usize = 8;

    fn next(&self, unit: usize) -> Option<usize> {
        if SIDE_FREELIST {
            return self.meta[unit].next();
        }
        let v = unsafe {
            self.unit_to_address(unit)
                .add(Self::NEXT_OFFSET)
                .load::<usize>()
        };
        if v == usize::MAX {
            None
        } else {
            Some(v)
        }
    }

    fn prev(&self, unit: usize) -> Option<usize> {
        if SIDE_FREELIST {
            return self.meta[unit].prev();
        }
        let v = unsafe {
            self.unit_to_address(unit)
                .add(Self::PREV_OFFSET)
                .load::<usize>()
        };
        if v == usize::MAX {
            None
        } else {
            Some(v)
        }
    }

    fn set_next(&mut self, unit: usize, next: Option<usize>) {
        if SIDE_FREELIST {
            return self.meta[unit].set_next(next);
        }
        let v = next.unwrap_or(usize::MAX);
        unsafe { self.unit_to_address(unit).add(Self::NEXT_OFFSET).store(v) }
    }

    fn set_prev(&mut self, unit: usize, prev: Option<usize>) {
        if SIDE_FREELIST {
            return self.meta[unit].set_prev(prev);
        }
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
        // println!("push cell {:?} sc={}", unit, sc);
        debug_assert!(self.next(unit).is_none());
        debug_assert!(self.prev(unit).is_none());
        let curr = self.heads[sc];
        if let Some(curr) = curr {
            self.set_prev(curr, Some(unit));
        }
        self.set_prev(unit, None);
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

    pub fn units_on_freelist(&self) -> usize {
        self.units_on_freelist
    }

    fn alloc_impl(&mut self, aligned_units: usize) -> Option<usize> {
        let sc = self.size_class(aligned_units);
        // println!("alloc {} units, sc={}", aligned_units, sc);
        if let Some(cell) = self.pop_cell(sc) {
            Some(cell)
        } else {
            let next_sc = sc + 1;
            if next_sc >= self.heads.len() {
                return None;
            }
            // alloc and split a larger cell
            let parent = self.alloc_impl(self.size_class_to_units(next_sc))?;
            let a = parent;
            let b = parent + self.size_class_to_units(sc);
            self.meta[a].set_size_class(sc);
            self.push_cell(b, sc);
            Some(a)
        }
    }

    pub fn alloc(&mut self, units: usize) -> Option<usize> {
        let aligned_units = units.next_power_of_two();
        let result = self.alloc_impl(aligned_units);
        if result.is_some() {
            self.units_on_freelist -= aligned_units;
        }
        result
    }

    fn free_impl(&mut self, unit: usize) -> FreeResult {
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
            let result = self.free_impl(parent);
            return FreeResult {
                units: 1 << sc,
                coalesced_units: result.coalesced_units,
                first_unit: result.first_unit,
            };
        } else {
            self.push_cell(unit, sc);
            debug_assert!(self.meta[unit].is_free());
            debug_assert!(self.meta[unit].size_class() == sc);
            return FreeResult {
                units: 1 << sc,
                coalesced_units: 1 << sc,
                first_unit: unit,
            };
        }
    }

    pub fn free(&mut self, unit: usize) -> FreeResult {
        let result = self.free_impl(unit);
        self.units_on_freelist += result.units;
        result
    }

    pub fn remove(&mut self, unit: usize) {
        let sc = self.meta[unit].size_class();
        self.remove_cell(unit, sc)
    }

    pub fn unit_to_address(&self, unit: usize) -> Address {
        debug_assert!(unit < SENTINEL);
        self.base + (unit << LOG_BYTES_IN_PAGE)
    }

    pub fn address_to_unit(&self, a: Address) -> usize {
        debug_assert!(a >= self.base);
        let unit = (a - self.base) >> LOG_BYTES_IN_PAGE;
        debug_assert!(unit < SENTINEL);
        unit
    }

    pub fn size(&self, unit: usize) -> usize {
        self.size_class_to_units(self.meta[unit].size_class())
    }

    pub fn insert(&mut self, unit: usize, units: usize) {
        let cap = unit + units.next_power_of_two();
        if self.meta.len() < cap {
            self.meta.resize(cap, CellMeta(0));
        }
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

#[derive(Debug)]
pub struct FreeResult {
    pub units: usize,
    pub coalesced_units: usize,
    pub first_unit: usize,
}
