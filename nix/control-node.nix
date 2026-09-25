{ self, inputs, ... }:
let
  nodeOptions = { lib, ... }: {
    options = {
      services.control-events-node = {
        enable = lib.mkEnableOption "a control-events-node on this machine, which listens for control-events";
        proxyTo = lib.mkOption {
          type = lib.types.nullOr lib.types.str;
          default = null;
          description = ''
            The server host to which events in this node are proxied
            to. The target node will typically aggregate events from various
            proxies, and mission centers or dashboards will query information
            from said target (though eventually falling back to this node if
                the potentially remote target is unavailable).
            '';
        };
        listenOn = lib.mkOption {
          type = lib.types.nullOr lib.types.str;
          default = null;
          description = ''
            If events are meant to be proxied to this node from other nodes,
               what address to bind the node on. "null" otherwise.

                 WARNING: use a private IP address like 10.0.0.3, within a private
                 (virtual) network. Currently, we rely on the VPN for security
                 rather than any TLS or password based authentication to the node.
                 '';
        };
        nodeId = lib.mkOption {
          type = lib.types.nullOr lib.types.str;
          default = null;
          description = ''
            String node identifier which must be unique across nodes
            proxying to the same target. Required if proxyTo is not null.
            '';
        };
      };
    };
  };
in
{
  flake.darwinModules.control-events = { config, lib, pkgs, ... }:
  let
    # Evaluate the nixos control-events module within the context of a full
    # nixosSystem using the darwin packages (to get the right hashes wherever
    # packages may be used in the config file).
    # The point is to reuse the services.mosquitto logic for free.
    nixosEvaluated = inputs.nixpkgs.lib.nixosSystem {
      modules = [
        self.nixosModules.control-events
        {
          nixpkgs.pkgs = pkgs;
          services.control-events-node = {
            inherit (cfg) enable proxyTo listenOn nodeId;
          };
        }
      ];
    };
    cfg = config.services.control-events-node;

    mosquitto      = nixosEvaluated.config.services.mosquitto;
    mosquitto_unit = nixosEvaluated.config.systemd.services.mosquitto;
  in
  {
    imports = [ nodeOptions ];
    config = lib.mkIf cfg.enable {
      launchd.daemons.mosquitto = {
        # Execute the exact same shell preStart and ExecStart commands as a
        # nixos system, but the pkgs referenced will be darwin pkgs!
        script = ''
          export PATH=${pkgs.coreutils}/bin:$PATH # for `install`
          mkdir -p ${mosquitto.dataDir}
          ${mosquitto_unit.preStart}
          exec ${mosquitto_unit.serviceConfig.ExecStart}
        '';
        serviceConfig = {
          RunAtLoad = true;
          KeepAlive = true;
          WorkingDirectory = mosquitto.dataDir;
          StandardOutPath = "/tmp/nix.services.mosquitto.out";
          StandardErrorPath = "/tmp/nix.services.mosquitto.err";
        };
      };
    };
  };

  flake.nixosModules.control-events = { config, lib, ... }:
  let cfg = config.services.control-events-node; in
  {

    imports = [ nodeOptions ];

    config = lib.mkIf cfg.enable {

      services.mosquitto = {
        enable = true;

        # Listeners specify how MQTT clients can connect to Mosquitto
        #
        # Allow readwrite any topic, without username nor passwords.
        # See WARNING on `cfg.listenOn`: we rely on the listen addr to be in a
        # (virtual) private net for security
        listeners =
        let
          simpl_listen = addr:
          {
            address = addr;
            acl = [ "pattern readwrite #" ];
            omitPasswordAuth = true;
            settings.allow_anonymous = true;
          };
        in
        [ (simpl_listen "127.0.0.1") ] # localhost
        ++
        lib.optional (cfg.listenOn != null)
          (simpl_listen cfg.listenOn); # should be a private ip

        # Bridges specify how to connect multiple MQTT brokers together
        # In our case, we always proxy topics out
        bridges = lib.mkIf (cfg.proxyTo != null) {
          "proxy_to" = {
            addresses = [{ address = cfg.proxyTo; }];
            topics = [ "# out 2" ];
            settings = {
              cleansession = false;
              remote_clientid = cfg.nodeId;
              bridge_protocol_version = "mqttv50";
            };
          };
        };

        # Persist messages
        persistence = true;
      };

      networking.firewall = lib.mkIf (cfg.listenOn != null) {
        # node is accessible from other machines, but mosquitto should only be
        # bound on private addr in `listenOn` (1883 is the default port)
        allowedTCPPorts = [ 1883 ];
      };

     };

  };
}
