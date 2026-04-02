use crate::prelude::*;
use cargo_test_support::basic_bin_manifest;
use cargo_test_support::cargo_test;
use cargo_test_support::project;

#[cargo_test]
fn warn_without_passing_unstable_flag() {
    let p = project()
        .file("Cargo.toml", &basic_bin_manifest("foo"))
        .file("src/main.rs", "fn main() {}")
        .file(
            ".cargo/config.toml",
            r#"
                [build.rbe]
                endpoint = "http://127.0.0.1:8980"
            "#,
        )
        .build();

    p.cargo("build")
        .masquerade_as_nightly_cargo(&["remote-reapi"])
        .with_stderr_data(cargo_test_support::str![[r#"
[WARNING] ignoring 'build.rbe' config, pass `-Zremote-reapi` to enable it
[COMPILING] foo v0.5.0 ([ROOT]/foo)
[FINISHED] `dev` profile [unoptimized + debuginfo] target(s) in [ELAPSED]s

"#]])
        .run();
}

#[cargo_test]
fn invalid_remote_endpoint_fails_without_fallback() {
    let p = project()
        .file("Cargo.toml", &basic_bin_manifest("foo"))
        .file("src/main.rs", "fn main() {}")
        .file(
            ".cargo/config.toml",
            r#"
                [build.rbe]
                endpoint = "not a url"
                fallback-local = false
            "#,
        )
        .build();

    p.cargo("build -Zremote-reapi")
        .masquerade_as_nightly_cargo(&["remote-reapi"])
        .with_status(101)
        .with_stderr_data(cargo_test_support::str![[r#"
[COMPILING] foo v0.5.0 ([ROOT]/foo)
[ERROR] could not compile `foo` (bin "foo")

Caused by:
  invalid `build.rbe.endpoint`

Caused by:
  invalid URI

Caused by:
  invalid uri character

"#]])
        .run();
}

#[cargo_test]
fn invalid_remote_endpoint_can_fall_back_to_local() {
    let p = project()
        .file("Cargo.toml", &basic_bin_manifest("foo"))
        .file("src/main.rs", "fn main() {}")
        .file(
            ".cargo/config.toml",
            r#"
                [build.rbe]
                endpoint = "not a url"
                fallback-local = true
            "#,
        )
        .build();

    p.cargo("build -Zremote-reapi")
        .masquerade_as_nightly_cargo(&["remote-reapi"])
        .run();

    assert!(p.bin("foo").is_file());
}
