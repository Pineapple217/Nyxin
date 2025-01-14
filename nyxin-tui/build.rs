fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("../proto/nyxin/api/chat/v1/base.proto")?;
    Ok(())
}
