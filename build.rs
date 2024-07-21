fn main() {
    built::write_built_file().expect("Failed to acquire build-time information");
    prost_build::compile_protos(
        &[
            "src/plan/lxr/shapes.proto",
            "src/plan/lxr/heapdump.proto",
        ],
        &["src/"],
    )
    .unwrap();
}
