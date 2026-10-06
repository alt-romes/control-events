{ self, ... }:
{
  systems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];

  perSystem = { pkgs, self', ... }: {
    packages.default = pkgs.haskell.lib.dontCheck (pkgs.haskellPackages.callCabal2nix "control-events" self { });

    packages.control-events = self'.packages.default;
  };
}
