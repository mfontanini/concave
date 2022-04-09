use std::io;

fn main() -> io::Result<()> {
    let protos = &["proto/object.proto"];
    prost_build::compile_protos(protos, &["proto/"])?;
    Ok(())
}
