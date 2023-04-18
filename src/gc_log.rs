use std::{
    cell::UnsafeCell,
    fmt::{self, Write},
};

#[macro_export]
macro_rules! gc_log {
    ([$level: literal] $($arg:tt)*) => {{
        if $crate::verbose($level) {
            $crate::gc_log::_log(format_args!($($arg)*), true, $level >= 3);
        }
    }};
    ($($arg:tt)*) => {{
        gc_log!([2] $($arg)*)
    }};
}

#[macro_export]
macro_rules! flush_logs {
    () => {{
        if $crate::verbose(1) {
            $crate::gc_log::_flush();
        }
    }};
}

thread_local! {
    pub static LOCAL_LOG_BUFFER: UnsafeCell<String> = UnsafeCell::new(String::new());
}

#[doc(hidden)]
#[cold]
pub fn _log(args: fmt::Arguments<'_>, new_line: bool, cached: bool) {
    LOCAL_LOG_BUFFER.with(|buf| {
        let buf = unsafe { &mut *buf.get() };
        buf.write_fmt(format_args!("[{:.3}s][info][gc] ", crate::boot_time_secs()))
            .unwrap();
        buf.write_fmt(args).unwrap();
        if new_line {
            buf.push('\n');
        }
    });
    if !cached || crate::scheduler::current_worker_ordinal().is_none() {
        _flush();
    }
}

#[doc(hidden)]
#[cold]
pub fn _flush() {
    LOCAL_LOG_BUFFER.with(|buf| {
        let buf = unsafe { &mut *buf.get() };
        if !buf.is_empty() {
            eprint!("{}", buf);
            buf.clear();
        }
    });
}
