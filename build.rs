fn main() {
    built::write_built_file().expect("Failed to acquire build-time information");
    prost_build::compile_protos(&["./shapes.proto", "./heapdump.proto"], &["."]).unwrap();
}
