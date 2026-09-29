{ self, ... }:
{
  # Like pkgs.writeShellScriptBin, but wrapped by
  # `control-events script <topic> -- ...`
  flake.lib.writeEventScriptBin = pkgs: topic: name: text:
    pkgs.writeShellScriptBin name ''
      exec ${self.packages.${pkgs.stdenv.hostPlatform.system}.default}/bin/control-events \
        script ${pkgs.lib.escapeShellArg topic} -- ${pkgs.writeShellScript name text} "$@"
    '';
}
