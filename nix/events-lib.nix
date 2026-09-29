{ self, ... }:
let
  # Like pkgs.writeShellScriptBin, but wrapped by
  # `control-events <cmd> <topic> -- ...`
  writeControlEventsBin = cmd: pkgs: topic: name: text:
    pkgs.writeShellScriptBin name ''
      exec ${self.packages.${pkgs.stdenv.hostPlatform.system}.default}/bin/control-events \
        ${cmd} ${pkgs.lib.escapeShellArg topic} -- ${pkgs.writeShellScript name text} "$@"
    '';
in
{
  flake.lib = {
    # Sends an event for the script run.
    writeEventScriptBin = writeControlEventsBin "script";
    # Sends a healthcheck event every minute while the script runs.
    writeHealthcheckScriptBin = writeControlEventsBin "healthcheck";
  };
}
