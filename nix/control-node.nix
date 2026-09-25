{ self, ... }:
{
  flake.darwinModules.control-node = { config, lib, pkgs, ... }:
  {};

  flake.nixosModules.control-node = { config, lib, pkgs, ... }:
  {

    options = {
      control-node = {
        enable = lib.mkEnableOption "Enable a control-node on this machine, which listens for control-events";
        proxyTo = {
          type = lib.nullOr lib.types.str;
          description = ''
            The server host to which events in this node are proxied
            to. The target node will typically aggregate events from various
            proxies, and mission centers or dashboards will query information
            from said target (though eventually falling back to this node if
            the potentially remote target is unavailable).
            '';
        };
        listenOn = {
          type = lib.nullOr lib.types.str;
          description = ''
            If events are meant to be proxied to this node from other nodes,
            what address to bind the node on. "null" otherwise.

            WARNING: use a private IP address like 10.0.0.3, within a private
            (virtual) network. Currently, we rely on the VPN for security
            rather than any TLS or password based authentication to the node.
            '';
        };
        nodeId = {
          type = lib.types.str;
          description = ''
            String node identifier which must be unique across nodes
            proxying to the same target node
            '';
        };
      };
    };

    config =
    let cfg = cfg.control-node;
     in {
      services.mosquitto = {
        enable = true;

        # Listeners specify how MQTT clients can connect to Mosquitto
        #
        # Allow readwrite any topic, without username nor passwords.
        # See WARNING on `cfg.listenOn`: we rely on the listen addr to be in a
        # (virtual) private net for security
        listeners =
        let
          simpl_listen = { addr }:
          {
            address = addr;
            acl = [ "pattern readwrite #" ];
            omitPasswordAuth = true;
            settings.allow_anonymous = true;
          };
        in
        [simpl_listen "127.0.0.1"]     # localhost
        ++
        lib.mkIf (cfg.listenOn != null)
          [simpl_listen cfg.listenOn]; # should be a private ip

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

      networking.firewall = lib.mkIf (config.control-node.listenOn) {
        enable = true;
        allowedTCPPorts = [ 1883 ]; # node is accessible from other machines,
                                    # but only on private addr in `listenOn`
                                    # (1883 is the default port)
      };
    };

  };
}
