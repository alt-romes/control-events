{ ... }:
{
  description = "Distributed control-events";

  inputs = {
    nixpkgs.url      = "github:NixOS/nixpkgs/nixos-unstable";
    flake-parts.url  = "github:hercules-ci/flake-parts";
    import-tree.url  = "github:vic/import-tree";

    flake-parts.inputs.nixpkgs.follows = "nixpkgs";
    import-tree.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs = inputs: inputs.flake-parts.lib.mkFlake { inherit inputs; }
    {
      imports = [ (inputs.import-tree [./nix]) ];
    };
}
