{ self, ... }:
let
  # Like pkgs.writeShellScriptBin, but wrapped by
  # `control-events <cmd> <opts> <topic> -- ...`
  writeControlEventsBin = cmd: pkgs:
    opts@{ expected ? null, timeout ? null, critical ? false, label ? null, content ? null }:
    topic: name: text:
    pkgs.writeShellScriptBin name ''
      exec ${self.packages.${pkgs.stdenv.hostPlatform.system}.control-events}/bin/control-events \
        ${cmd} ${pkgs.lib.cli.toCommandLineShellGNU { } opts} ${pkgs.lib.escapeShellArg topic} -- ${pkgs.writeShellScript name text} "$@"
    '';
in
{
  flake.lib = {
    # Sends an event for the script run.
    writeEventScriptBin = writeControlEventsBin "script";
    # Sends a healthcheck event every `expected` seconds while the script runs.
    writeHealthcheckScriptBin = writeControlEventsBin "healthcheck";

    # Like writeEventScriptBin, but using micro-control-events.
    writeMicroEventScriptBin = pkgs:
      { expected ? null, timeout ? null, critical ? false, label ? null, content ? null }:
      topic: name: text:
      let e = x: pkgs.lib.escapeShellArg (toString x); in
      pkgs.writeShellScriptBin name ''
        TOPIC=${e "script/${topic}"} LABEL=${e label} EXPECTED=${e expected} TIMEOUT=${e timeout} \
        CRITICAL=${pkgs.lib.boolToString critical} CONTENT=${e content} \
          exec ${self.packages.${pkgs.stdenv.hostPlatform.system}.micro-control-events}/bin/micro-control-events ${pkgs.writeShellScript name text} "$@"
      '';
  };
}
