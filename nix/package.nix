{ self, inputs, ... }:
let
  # Sends a $TOPIC event for the run of EXE:
  #   TOPIC=script/foo [LABEL=...] [TIMEOUT=300] [EXPECTED=SECS] [CRITICAL=true] [CONTENT=JSON] \
  #     micro-control-events EXE [ARGS...]
  #
  # It only needs `mosquitto_pub` (ie no haskell, so it's good for arm32
  # embedded systems).
  micro-control-events = { writeShellScriptBin, lib, mosquitto, coreutils }:
    writeShellScriptBin "micro-control-events" ''
      export PATH=${lib.makeBinPath [ mosquitto coreutils ]}:/usr/bin:$PATH
      : "''${TOPIC:?must be set}" "''${1:?usage: TOPIC=... micro-control-events EXE [ARGS...]}"

      ID=$(cat /proc/sys/kernel/random/uuid 2>/dev/null || uuidgen)

      pub() {
        mosquitto_pub -h 127.0.0.1 -V mqttv5 -q 2 \
          -D publish correlation-data "$ID" \
          -t "$TOPIC/$1" \
          -m "{\"at\":\"$(date -u +%FT%TZ)\",\"e\":$2}"
      }

      label=''${LABEL//\\/\\\\}
      label=''${label//\"/\\\"}
      rules="\"timeout\":''${TIMEOUT:-300}''${EXPECTED:+,\"expected\":$EXPECTED},\"critical\":''${CRITICAL:-false}"
      pub start "{\"label\":\"$label\",\"rules\":{$rules}''${CONTENT:+,\"content\":$CONTENT}}" || exit 1

      "$@"
      ec=$?
      if [ $ec -eq 0 ]; then
        done='{"summary":"Ran successfully","success":true}'
      else
        done="{\"summary\":\"Failed with exit code $ec\",\"success\":false}"
      fi
      pub finished "$done" || exit 1
      exit $ec
    '';
in
{
  systems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];

  # Raspberry Pi Zero, cross-compiled
  flake.packages.armv6l-linux.micro-control-events =
    inputs.nixpkgs.legacyPackages.aarch64-linux.pkgsCross.raspberryPi.callPackage micro-control-events { };

  perSystem = { pkgs, self', ... }: {
    packages.default = pkgs.haskell.lib.dontCheck (pkgs.haskellPackages.callCabal2nix "control-events" self { });

    packages.control-events = self'.packages.default;

    packages.micro-control-events = pkgs.callPackage micro-control-events { };
  };
}
