{
  pkgs,
  lib,
  config,
  inputs,
  ...
}:

{
  languages.rust = {
    enable = true;
    channel = "nightly";
    mold.enable = true;
    components = [
      "rustc"
      "cargo"
      "rust-analyzer"
      "rustfmt"
      "clippy"
    ];
  };

  packages = with pkgs; [
    bacon
    cargo-nextest
    cargo-cache
    cargo-deny
  ];

  git-hooks.hooks = {
    typos.enable = true;
    clippy.enable = true;
  };

  scripts.watcher = {
    exec = ''
      watchexec -r -c -e rs \
      "cargo clippy && cargo test && cargo run"
    '';
    packages = [ pkgs.watchexec ];
  };

  # See full reference at https://devenv.sh/reference/options/
}
