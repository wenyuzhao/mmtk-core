use atomic::{Atomic, Ordering};

pub trait ExponentialDecayUpdatable: Copy {
    fn update(old: Self, current: Self) -> Self;
}

pub struct ExponentialDecayValue<T> {
    value: Atomic<T>,
}

impl<T> ExponentialDecayValue<T> {
    pub const fn new(initial: T) -> Self {
        Self {
            value: Atomic::new(initial),
        }
    }
}

impl<T: ExponentialDecayUpdatable> ExponentialDecayValue<T> {
    #[inline(always)]
    pub fn get(&self) -> T {
        self.value.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn update(&self, current_value: T) -> T {
        let old_value = self.value.load(Ordering::SeqCst);
        let new_value = T::update(old_value, current_value);
        self.value.store(new_value, Ordering::SeqCst);
        new_value
    }
}

macro_rules! update_generic {
    ($prev: expr, $curr: expr, $weight: expr) => {
        if $curr > $prev {
            ($curr + $prev * $weight) / ($weight + 1.0)
        } else {
            ($weight * $curr + $prev) / ($weight + 1.0)
        }
    };
}

impl ExponentialDecayUpdatable for f32 {
    #[inline(always)]
    fn update(old: Self, current: Self) -> Self {
        update_generic!(old, current, 3.0)
    }
}

impl ExponentialDecayUpdatable for f64 {
    #[inline(always)]
    fn update(old: Self, current: Self) -> Self {
        update_generic!(old, current, 3.0)
    }
}
