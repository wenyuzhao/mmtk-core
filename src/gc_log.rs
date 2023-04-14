#[macro_export]
macro_rules! gc_log {
    ([$level: literal] $($arg:tt)*) => {{
        if $crate::verbose($level) {
            eprint!("[{:.3}s][info][gc] ", crate::boot_time_secs());
            eprintln!($($arg)*)
        }
    }};
    ($($arg:tt)*) => {{
        gc_log!([2] $($arg)*)
    }};
}
