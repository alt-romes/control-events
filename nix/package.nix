{ self, ... }:
{
  systems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];

  perSystem = { pkgs, ... }: {
    packages.default =
      pkgs.haskell.lib.justStaticExecutables
        (pkgs.haskellPackages.callCabal2nix "control-events" self { });
  };
}
