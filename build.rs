use {
    cargo_lock::Lockfile, std::collections::HashSet,
};

fn main() -> anyhow::Result<()> {
    vergen::Emitter::default()
        .add_instructions(&vergen::BuildBuilder::all_build()?)?
        .add_instructions(&vergen::RustcBuilder::all_rustc()?)?
        .emit()?;

    // vergen git version does not looks cool
    println!(
        "cargo:rustc-env=GIT_VERSION={}",
        git_version::git_version!()
    );

    // Extract packages version
    let lockfile = Lockfile::load("Cargo.lock")?;
    println!(
        "cargo:rustc-env=SOLANA_SDK_VERSION={}",
        get_pkg_version(&lockfile, "solana-sdk")
    );
    println!(
        "cargo:rustc-env=YELLOWSTONE_GRPC_PROTO_VERSION={}",
        get_pkg_version(&lockfile, "yellowstone-grpc-proto")
    );

    std::env::set_var("PROTOC", protobuf_src::protoc());

    // build protos
    tonic_build::configure()
        .type_attribute("geyser.SubscribeUpdateTransactionInfo", "#[derive(serde::Serialize, serde::Deserialize)]")
        .type_attribute("solana.storage.ConfirmedBlock.Transaction", "#[derive(serde::Serialize, serde::Deserialize)]")
        .type_attribute("solana.storage.ConfirmedBlock.TransactionStatusMeta", "#[derive(serde::Serialize, serde::Deserialize)]")
        .type_attribute("solana.storage.ConfirmedBlock.ReturnData", "#[derive(serde::Serialize, serde::Deserialize)]")
        .type_attribute("solana.storage.ConfirmedBlock.Reward", "#[derive(serde::Serialize, serde::Deserialize)]")
        .type_attribute("solana.storage.ConfirmedBlock.TokenBalance", "#[derive(serde::Serialize, serde::Deserialize)]")
        .type_attribute("solana.storage.ConfirmedBlock.InnerInstructions", "#[derive(serde::Serialize, serde::Deserialize)]")
        .type_attribute("solana.storage.ConfirmedBlock.TransactionError", "#[derive(serde::Serialize, serde::Deserialize)]")
        .type_attribute("solana.storage.ConfirmedBlock.UiTokenAmount", "#[derive(serde::Serialize, serde::Deserialize)]")
        .type_attribute("solana.storage.ConfirmedBlock.InnerInstruction", "#[derive(serde::Serialize, serde::Deserialize)]")
        .type_attribute("solana.storage.ConfirmedBlock.Message", "#[derive(serde::Serialize, serde::Deserialize)]")
        .type_attribute("solana.storage.ConfirmedBlock.MessageAddressTableLookup", "#[derive(serde::Serialize, serde::Deserialize)]")
        .type_attribute("solana.storage.ConfirmedBlock.CompiledInstruction", "#[derive(serde::Serialize, serde::Deserialize)]")
        .type_attribute("solana.storage.ConfirmedBlock.MessageHeader", "#[derive(serde::Serialize, serde::Deserialize)]")
        .compile_protos(&["proto/geyser.proto"], &["proto"])?;
    Ok(())
}

fn get_pkg_version(lockfile: &Lockfile, pkg_name: &str) -> String {
    lockfile
        .packages
        .iter()
        .filter(|pkg| pkg.name.as_str() == pkg_name)
        .map(|pkg| pkg.version.to_string())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>()
        .join(",")
}
