// Vendored protoc means a contributor never has to apt-install protobuf-compiler.
// Phase 0 decision: tonic-build at compile time, no check-in of generated code.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc);

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["../../proto/heartbeat.proto"], &["../../proto"])?;

    println!("cargo:rerun-if-changed=../../proto/heartbeat.proto");
    Ok(())
}
