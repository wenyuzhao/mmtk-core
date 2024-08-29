// ADAPTED FROM: https://github.com/kinghajj/deque

// Copyright 2013 The Rust Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! A (mostly) lock-free concurrent work-stealing deque
//!
//! This module contains an implementation of the Chase-Lev work stealing deque
//! described in "Dynamic Circular Work-Stealing Deque". The implementation is
//! heavily based on the implementation using C11 atomics in "Correct and
//! Efficient Work Stealing for Weak Memory Models".
//!
//! The only potentially lock-synchronized portion of this deque is the
//! occasional call to the memory allocator when growing the deque. Otherwise
//! all operations are lock-free.
//!
//! # Example
//!
//!     use deque;
//!
//!     let (worker, stealer) = deque::new();
//!
//!     // Only the worker may push/pop
//!     worker.push(1);
//!     worker.pop();
//!
//!     // Stealers take data from the other end of the deque
//!     worker.push(1);
//!     stealer.steal();
//!
//!     // Stealers can be cloned to have many stealers stealing in parallel
//!     worker.push(1);
//!     let stealer2 = stealer.clone();
//!     stealer2.steal();

pub use self::Stolen::*;

use std::arch::asm;
use std::cell::Cell;
use std::fmt;
use std::marker::PhantomData;
use std::mem::{forget, MaybeUninit};
use std::ptr;
use std::sync::Arc;

use std::sync::atomic::Ordering::{self, Acquire, Relaxed, Release, SeqCst};
use std::sync::atomic::{fence, AtomicIsize, AtomicPtr};

fn mfence() {
    if cfg!(feature = "fast_fence") {
        unsafe {
            asm!("lock add dword ptr [rsp], 0");
        }
        std::sync::atomic::compiler_fence(Ordering::SeqCst);
    } else {
        fence(SeqCst);
    }
}

// Initial size for a buffer.
const MIN_SIZE: usize = 64;

struct Deque<T: Send> {
    bottom: AtomicIsize,
    top: AtomicIsize,
    array: AtomicPtr<Buffer<T>>,
}

/// Worker half of the work-stealing deque. This worker has exclusive access to
/// one side of the deque, and uses `push` and `pop` method to manipulate it.
///
/// There may only be one worker per deque.
pub struct Worker<T: Send> {
    deque: Arc<Deque<T>>,
    // Marker so that the Worker is Send but not Sync. The worker can only be
    // accessed from a single thread at once. Ideally we would use a negative
    // impl here but these are not stable yet.
    marker: PhantomData<Cell<()>>,
}

/// The stealing half of the work-stealing deque. Stealers have access to the
/// opposite end of the deque from the worker, and they only have access to the
/// `steal` method.
pub struct Stealer<T: Send> {
    deque: Arc<Deque<T>>,
    _padding: [u8; 256],
}

impl<T: Send> Clone for Stealer<T> {
    fn clone(&self) -> Self {
        Stealer {
            deque: self.deque.clone(),
            _padding: [0u8; 256],
        }
    }
}

/// When stealing some data, this is an enumeration of the possible outcomes.
#[derive(PartialEq, Debug)]
pub enum Stolen<T> {
    /// The deque was empty at the time of stealing
    Empty,
    /// The stealer lost the race for stealing data, and a retry may return more
    /// data.
    Abort,
    /// The stealer has successfully stolen some data.
    Data(T),
}

/// An internal buffer used by the chase-lev deque. This structure is actually
/// implemented as a circular buffer, and is used as the intermediate storage of
/// the data in the deque.
///
/// This type is implemented with *T instead of Vec<T> for two reasons:
///
///   1. There is nothing safe about using this buffer. This easily allows the
///      same value to be read twice in to rust, and there is nothing to
///      prevent this. The usage by the deque must ensure that one of the
///      values is forgotten. Furthermore, we only ever want to manually run
///      destructors for values in this buffer (on drop) because the bounds
///      are defined by the deque it's owned by.
///
///   2. We can certainly avoid bounds checks using *T instead of Vec<T>, although
///      LLVM is probably pretty good at doing this already.
///
/// Note that we keep old buffers around after growing because stealers may still
/// be concurrently accessing them. The buffers are kept in a linked list, with
/// each buffer pointing to the previous, smaller buffer. This doesn't leak any
/// memory because all buffers in the list are freed when the deque is dropped.
struct Buffer<T: Send> {
    storage: *mut T,
    size: usize,
    prev: Option<Box<Buffer<T>>>,
}

/// Allocates a new work-stealing deque.
pub fn new<T: Send>() -> (Worker<T>, Stealer<T>) {
    let a = Arc::new(Deque::new());
    let b = a.clone();
    (
        Worker {
            deque: a,
            marker: PhantomData,
        },
        Stealer {
            deque: b,
            _padding: [0u8; 256],
        },
    )
}

impl<T: Send> Worker<T> {
    /// Pushes data onto the front of this work queue.
    #[inline(always)]
    pub fn push(&mut self, t: T) -> bool {
        unsafe { self.deque.push(t).is_ok() }
    }
    /// Pops data off the front of the work queue, returning `None` on an empty
    /// queue.
    #[inline(always)]
    pub fn pop(&mut self) -> Option<T> {
        unsafe { self.deque.pop() }
    }
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        let b = self.deque.bottom.load(Relaxed);
        let t = self.deque.top.load(Relaxed);
        b.wrapping_sub(t) <= 0
    }
    #[inline(always)]
    pub fn pop_bulk<const N: usize>(&mut self) -> Option<([MaybeUninit<T>; N], usize)> {
        unsafe { self.deque.pop_bulk() }
    }
    pub fn new() -> Worker<T> {
        let a = Arc::new(Deque::new());
        Worker {
            deque: a,
            marker: PhantomData,
        }
    }
    pub fn stealer(&self) -> Stealer<T> {
        Stealer {
            deque: self.deque.clone(),
            _padding: [0u8; 256],
        }
    }
}

impl<T: Send> Stealer<T> {
    /// Steals work off the end of the queue (opposite of the worker's end)
    #[inline(always)]
    pub fn steal(&self) -> Stolen<T> {
        unsafe { self.deque.steal() }
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        let b = self.deque.bottom.load(Relaxed);
        let t = self.deque.top.load(Relaxed);
        b.wrapping_sub(t) <= 0
    }
}

impl<T: Send> Deque<T> {
    fn new() -> Deque<T> {
        let buf = Box::new(unsafe { Buffer::new(MIN_SIZE) });
        Deque {
            bottom: AtomicIsize::new(0),
            top: AtomicIsize::new(0),
            array: AtomicPtr::new(Box::into_raw(buf)),
        }
    }

    #[inline(always)]
    unsafe fn push(&self, data: T) -> Result<(), T> {
        let b = self.bottom.load(Relaxed);
        let t = self.top.load(Acquire);
        let a = self.array.load(Relaxed);

        // Grow the buffer if it is full.
        let size = b.wrapping_sub(t);
        if size == (*a).size() {
            return Err(data);
        }

        (*a).put(b, data);
        fence(Release);
        self.bottom.store(b.wrapping_add(1), Relaxed);
        Ok(())
    }

    #[inline(always)]
    unsafe fn pop_bulk<const N: usize>(&self) -> Option<([MaybeUninit<T>; N], usize)> {
        let n = N as isize;
        let b = self.bottom.load(Relaxed);

        // Early exit if the deque is empty. This avoids the need for a SeqCst
        // fence in this case.
        let t = self.top.load(Relaxed);
        if b.wrapping_sub(t) <= 0 {
            return None;
        }

        // Make sure bottom is stored before top is read.
        let m = isize::min(b.wrapping_sub(t), n);
        let b = b.wrapping_sub(m);
        self.bottom.store(b, Relaxed);
        mfence();
        let t = self.top.load(Relaxed);

        // If the deque is empty, restore bottom and exit.
        let size = b.wrapping_sub(t);
        if size < 0 {
            self.bottom.store(b.wrapping_add(m), Relaxed);
            return None;
        }

        // Fetch the element from the queue.
        let a = self.array.load(Relaxed);
        let mut data: [MaybeUninit<T>; N] = [const { MaybeUninit::uninit() }; N];
        for i in 0..m {
            let e = (*a).get(b.wrapping_add(i));
            data[i as usize].write(e);
        }

        // If this was the last element in the queue, check for races.
        if size != 0 {
            return Some((data, m as usize));
        }
        if self
            .top
            .compare_exchange(t, t.wrapping_add(m), SeqCst, SeqCst)
            .is_ok()
        {
            self.bottom.store(t.wrapping_add(m), Relaxed);
            return Some((data, m as usize));
        } else {
            self.bottom.store(t.wrapping_add(m), Relaxed);
            forget(data); // Someone else stole this value
            return None;
        }
    }

    #[inline(always)]
    unsafe fn pop(&self) -> Option<T> {
        let b = self.bottom.load(Relaxed);

        // Early exit if the deque is empty. This avoids the need for a SeqCst
        // fence in this case.
        let t = self.top.load(Relaxed);
        if b.wrapping_sub(t) <= 0 {
            return None;
        }

        // Make sure bottom is stored before top is read.
        let b = b.wrapping_sub(1);
        self.bottom.store(b, Relaxed);
        mfence();
        let t = self.top.load(Relaxed);

        // If the deque is empty, restore bottom and exit.
        let size = b.wrapping_sub(t);
        if size < 0 {
            self.bottom.store(b.wrapping_add(1), Relaxed);
            return None;
        }

        // Fetch the element from the queue.
        let a = self.array.load(Relaxed);
        let data = (*a).get(b);

        // If this was the last element in the queue, check for races.
        if size != 0 {
            return Some(data);
        }
        if self
            .top
            .compare_exchange(t, t.wrapping_add(1), SeqCst, SeqCst)
            .is_ok()
        {
            self.bottom.store(t.wrapping_add(1), Relaxed);
            return Some(data);
        } else {
            self.bottom.store(t.wrapping_add(1), Relaxed);
            forget(data); // Someone else stole this value
            return None;
        }
    }

    #[inline(always)]
    unsafe fn steal(&self) -> Stolen<T> {
        // Make sure top is read before bottom.
        let t = self.top.load(Acquire);
        mfence();
        let b = self.bottom.load(Acquire);

        // Exit if the queue is empty.
        let size = b.wrapping_sub(t);
        if size <= 0 {
            return Empty;
        }

        // Fetch the element from the queue.
        let a = self.array.load(Acquire);
        let data = (*a).get(t);

        // Attempt to increment top.
        if self
            .top
            .compare_exchange(t, t.wrapping_add(1), SeqCst, SeqCst)
            .is_ok()
        {
            Data(data)
        } else {
            forget(data); // Someone else stole this value
            Abort
        }
    }
}

impl<T: Send> Drop for Deque<T> {
    fn drop(&mut self) {
        let t = self.top.load(Relaxed);
        let b = self.bottom.load(Relaxed);
        let a = self.array.load(Relaxed);

        // Free whatever is leftover in the deque, and then free the buffer.
        // This will also free all linked buffers.
        let mut i = t;
        while i != b {
            unsafe { (*a).get(i) };
            i = i.wrapping_add(1);
        }
        let _ = unsafe { Box::from_raw(a) };
    }
}

#[inline]
unsafe fn take_ptr_from_vec<T>(mut buf: Vec<T>) -> *mut T {
    let ptr = buf.as_mut_ptr();
    forget(buf);
    ptr
}

#[inline]
unsafe fn allocate<T>(number: usize) -> *mut T {
    let v = Vec::with_capacity(number);
    take_ptr_from_vec(v)
}

#[inline]
unsafe fn deallocate<T>(ptr: *mut T, number: usize) {
    Vec::from_raw_parts(ptr, 0, number);
}

impl<T: Send> Buffer<T> {
    unsafe fn new(size: usize) -> Buffer<T> {
        Buffer {
            storage: allocate(size),
            size: size,
            prev: None,
        }
    }

    fn size(&self) -> isize {
        self.size as isize
    }

    fn mask(&self) -> isize {
        self.size as isize - 1
    }

    unsafe fn elem(&self, i: isize) -> *mut T {
        self.storage.offset(i & self.mask())
    }

    // This does not protect against loading duplicate values of the same cell,
    // nor does this clear out the contents contained within. Hence, this is a
    // very unsafe method which the caller needs to treat specially in case a
    // race is lost.
    unsafe fn get(&self, i: isize) -> T {
        ptr::read(self.elem(i))
    }

    // Unsafe because this unsafely overwrites possibly uninitialized or
    // initialized data.
    unsafe fn put(&self, i: isize, t: T) {
        ptr::write(self.elem(i), t);
    }

    // Again, unsafe because this has incredibly dubious ownership violations.
    // It is assumed that this buffer is immediately dropped.
    unsafe fn grow(self: Box<Buffer<T>>, b: isize, t: isize) -> Box<Buffer<T>> {
        let mut buf = Box::new(Buffer::new(self.size * 2));
        let mut i = t;
        while i != b {
            buf.put(i, self.get(i));
            i = i.wrapping_add(1);
        }
        buf.prev = Some(self);
        return buf;
    }
}

impl<T: Send> Drop for Buffer<T> {
    fn drop(&mut self) {
        // It is assumed that all buffers are empty on drop.
        unsafe { deallocate(self.storage, self.size) }
    }
}

impl<T: Send> fmt::Debug for Deque<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> fmt::Result {
        f.debug_struct("Deque")
            .field("bottom", &self.bottom)
            .field("top", &self.top)
            .field("array", &self.array)
            .finish()
    }
}

impl<T: Send> fmt::Debug for Worker<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> fmt::Result {
        f.debug_struct("Worker")
            .field("deque", &self.deque)
            .field("marker", &self.marker)
            .finish()
    }
}

impl<T: Send> fmt::Debug for Stealer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> fmt::Result {
        f.debug_struct("Stealer")
            .field("deque", &self.deque)
            .finish()
    }
}
