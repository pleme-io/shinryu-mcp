{
  description = "Shinryū analytical query plane — DataFusion MCP server for cross-signal SQL analysis";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.11";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    substrate = {
      url = "github:pleme-io/substrate";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.fenix.follows = "fenix";
    };
    forge = {
      url = "github:pleme-io/forge";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.fenix.follows = "fenix";
      inputs.substrate.follows = "substrate";
      inputs.crate2nix.follows = "crate2nix";
    };
    crate2nix = {
      url = "github:nix-community/crate2nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    devenv = {
      url = "github:cachix/devenv";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, substrate, forge, crate2nix, devenv, ... }:
    (import "${substrate}/lib/rust-service-flake.nix" {
      inherit nixpkgs substrate forge crate2nix devenv;
    }) {
      inherit self;
      serviceName = "shinryu-mcp";
      registry = "ghcr.io/pleme-io/shinryu-mcp";
      packageName = "shinryu-mcp";
      namespace = "observability";
      architectures = [ "arm64" ];
      serviceType = "rest";
      ports = {
        http = 8080;
        health = 8081;
        metrics = 9090;
      };
      crateOverrides = {
        rmcp = attrs: {
          CARGO_CRATE_NAME = "rmcp";
        };
      };
      moduleDir = ./module;
    };
}
