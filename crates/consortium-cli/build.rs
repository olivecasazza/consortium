//! Build-time registry facts: parse the crate's `Cargo.toml` `[[bin]]`
//! table and snapshot name, declared source path, and source/test file
//! presence into a generated const consumed by `grammar.rs`, so runtime
//! checks never touch the build source tree.
use std::path::Path;

fn main() {
    println!("cargo:rerun-if-changed=Cargo.toml");
    println!("cargo:rerun-if-changed=src/bin");
    println!("cargo:rerun-if-changed=tests");

    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let bins = read_manifest_bins(&manifest.join("Cargo.toml"))
        .expect("parse CLI bin registry from Cargo.toml");

    let mut facts = String::from("pub(crate) const REGISTRY_FACTS: &[RegistryFact] = &[\n");
    for (name, path) in &bins {
        let us = name.replace('-', "_");
        let source = manifest.join(path).exists();
        let tests = manifest
            .join("tests")
            .join(format!("{us}_tests.rs"))
            .exists();
        facts.push_str(&format!(
            "    RegistryFact {{ name: {name:?}, path: {path:?}, source_exists: {source}, \
             tests_exist: {tests} }},\n"
        ));
    }
    facts.push_str("];\n");
    emit(&facts);
    emit_integrations(&manifest.join("Cargo.toml"));
}

/// `[[bin]]` entries parsed from a Cargo.toml manifest, in declaration
/// order: `(name, path)` with path relative to the manifest directory
/// (Cargo's default `src/bin/<name>.rs` when omitted). No hardcoded bin
/// list here: the manifest is the source of truth.
fn read_manifest_bins(
    manifest: &Path,
) -> Result<Vec<(String, String)>, Box<dyn std::error::Error>> {
    let raw = std::fs::read_to_string(manifest)?;
    let value: toml::Value = toml::from_str(&raw)?;
    let bins = value
        .get("bin")
        .and_then(|b| b.as_array())
        .map(|a| a.as_slice())
        .unwrap_or(&[]);
    Ok(bins
        .iter()
        .filter_map(|b| {
            let name = b.get("name")?.as_str()?.to_string();
            let path = b
                .get("path")
                .and_then(|p| p.as_str())
                .map(|p| p.to_string())
                .unwrap_or_else(|| format!("src/bin/{name}.rs"));
            Some((name, path))
        })
        .collect())
}

fn emit(body: &str) {
    let out = std::env::var("OUT_DIR").expect("OUT_DIR set by cargo");
    std::fs::write(Path::new(&out).join("grammar_registry.rs"), body).expect("write OUT_DIR");
}

fn emit_integrations(manifest: &Path) {
    let source = std::fs::read_to_string(manifest).expect("read CLI manifest");
    let value: toml::Value = toml::from_str(&source).expect("parse CLI manifest");
    let registry = value["package"]["metadata"]["consortium"]["integrations"]
        .as_table()
        .expect("CLI manifest must declare integration registry");
    assert!(
        !registry.is_empty(),
        "integration registry must not be empty"
    );
    let mut code = String::from("pub fn integrations() -> Vec<Box<dyn Integration>> { vec![\n");
    for (dependency, adapter_type) in registry {
        assert!(
            value["dependencies"].get(dependency).is_some(),
            "integration {dependency} is not a CLI dependency"
        );
        let adapter_type = adapter_type
            .as_str()
            .expect("integration adapter type must be a string");
        let crate_name = dependency.replace('-', "_");
        for identifier in [&crate_name, adapter_type] {
            assert!(
                !identifier.is_empty()
                    && identifier
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'_'),
                "invalid integration identifier: {identifier}"
            );
        }
        code.push_str(&format!("Box::new({crate_name}::{adapter_type}),\n"));
    }
    code.push_str("] }\n");
    let out = std::env::var("OUT_DIR").expect("OUT_DIR set by cargo");
    std::fs::write(Path::new(&out).join("integration_registry.rs"), code)
        .expect("write integration registry");
}
