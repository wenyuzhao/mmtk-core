use std::mem::MaybeUninit;

pub struct Stack<T> {
    data: Vec<Vec<T>>,
    cache: Vec<Vec<T>>,
    max_cache: usize,
}

impl<T> Stack<T> {
    pub fn new() -> Self {
        Self {
            data: Vec::new(),
            cache: Vec::new(),
            max_cache: 4,
        }
    }

    fn alloc(&mut self) -> Vec<T> {
        if let Some(cache) = self.cache.pop() {
            cache
        } else {
            Vec::with_capacity(Self::segment_size())
        }
    }

    const fn segment_size() -> usize {
        4096 / std::mem::size_of::<T>()
    }

    #[inline(always)]
    pub fn push(&mut self, item: T) {
        if self.data.is_empty() {
            let new_buf = self.alloc();
            self.data.push(new_buf);
        }
        self.data.last_mut().unwrap().push(item);
    }

    #[inline(always)]
    pub fn pop(&mut self) -> Option<T> {
        if self.data.is_empty() {
            return None;
        }
        let item = self.data.last_mut().unwrap().pop();
        if self.data.last().unwrap().is_empty() {
            let buf = self.data.pop();
            if self.cache.len() < self.max_cache {
                self.cache.push(buf.unwrap());
            } else {
                drop(buf);
            }
        }
        item
    }

    #[inline(always)]
    pub fn pop_bulk<const N: usize>(&mut self) -> Option<([MaybeUninit<T>; N], usize)> {
        if self.data.is_empty() {
            return None;
        }
        let mut buf = [const { MaybeUninit::uninit() }; N];
        let mut i = 0;
        while i < N {
            if let Some(item) = self.pop() {
                buf[i] = MaybeUninit::new(item);
                i += 1;
            } else {
                break;
            }
        }
        Some((buf, i))
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}
