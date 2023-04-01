#[macro_export]
macro_rules! gc_log {
    ($($arg:tt)*) => {{
        eprint!("[{:.3}s][info][gc] ", crate::boot_time_secs());
        eprintln!($($arg)*)
    }};
}
