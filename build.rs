use std::io;

fn main() -> io::Result<()> {
    let protos = &["proto/storage.proto"];
    prost_build::compile_protos(protos, &["proto/"])?;
    Ok(())
}
