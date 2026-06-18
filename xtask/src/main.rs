use std::{env, process::Command};

fn main() -> anyhow::Result<()> {
    let task = env::args().nth(1).unwrap_or_default();
    match task.as_str() {
        "gen-proto" => gen_proto(),
        "gen-crds" => gen_crds(),
        "check-crd-drift" => check_crd_drift(),
        "fmt-check" => run(&["cargo", "fmt", "--check"]),
        "ci" => ci(),
        other => Err(anyhow::anyhow!(
            "unknown xtask: {other:?}. \
             try: gen-proto | gen-crds | check-crd-drift | fmt-check | ci"
        )),
    }
}

fn gen_proto() -> anyhow::Result<()> {
    // tonic-build runs inside sk-broker's build.rs. Forcing a rebuild here means
    // the generated code lands in target/ regardless of cargo's incremental cache.
    run(&["cargo", "build", "-p", "sk-broker"])
}

fn gen_crds() -> anyhow::Result<()> {
    // Phase 0 stub. Phase 7 replaces this with a schemars walker over
    // sk-operator-api that writes deploy/crds/*.yaml + mirrors into the chart.
    eprintln!("gen-crds: stub — implemented in Phase 7");
    Ok(())
}

fn check_crd_drift() -> anyhow::Result<()> {
    gen_crds()?;
    run(&[
        "git",
        "diff",
        "--exit-code",
        "deploy/crds",
        "deploy/helm/skafka/crds",
    ])
}

fn ci() -> anyhow::Result<()> {
    run(&["cargo", "fmt", "--check"])?;
    run(&[
        "cargo",
        "clippy",
        "--workspace",
        "--all-targets",
        "--",
        "-D",
        "warnings",
    ])?;
    run(&["cargo", "test", "--workspace"])?;
    run(&["cargo", "build", "--release", "--workspace", "--bins"])?;
    Ok(())
}

fn run(argv: &[&str]) -> anyhow::Result<()> {
    let status = Command::new(argv[0]).args(&argv[1..]).status()?;
    if !status.success() {
        anyhow::bail!("{:?} failed: {status}", argv);
    }
    Ok(())
}
