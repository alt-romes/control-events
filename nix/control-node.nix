{ self, inputs, ... }:
let
  nodeOptions = { lib, pkgs, ... }: {
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
          type = lib.types.str;
          description = ''
            String node identifier which must be unique across nodes
            proxying to the same target. It's also the name under which
            system-healthcheck pings.
            '';
        };

        bridge2 = lib.mkOption {
          type = lib.types.bool;
          default = false;
          description = ''
            Whether to additionally bridge events from the `proxyTo` target
            back into this node.
            '';
        };

        system-healthcheck = lib.mkEnableOption "Enable a periodic system/<name> event with some system stats to observe load.";

        package = lib.mkOption {
          type = lib.types.nullOr lib.types.package;
          default = self.packages.${pkgs.stdenv.hostPlatform.system}.default;
          description = "The control-events package to use and install.";
        };

      };
    };
  };

  # Mosquitto 2.0.23 fixed "handling of disconnected sessions for
  # `per_listener_settings true`" which is always true in this config.
  # This fix is critical because messages for a sleeping machine would get
  # silently dropped. And, the sqlite persistence plugin needs 2.1.
  mosquittoAssertion = lib: mosquitto: {
    assertion = lib.versionAtLeast mosquitto.version "2.1";
    message = "control-events-node needs mosquitto >= 2.1, but services.mosquitto.package is ${mosquitto.version}.";
  };

  # Send a system/<name> event with load stats to the local broker
  healthcheckLoop = pkgs: mosquitto: name: pkgs.writeShellScript "control-events-system-healthcheck" ''
    export PATH=${pkgs.lib.makeBinPath [ mosquitto pkgs.libstatgrab pkgs.coreutils ]}:/usr/bin:$PATH
    NAME=${pkgs.lib.escapeShellArg name}

    pub() {
      mosquitto_pub -h 127.0.0.1 -V mqttv5 -q 2 \
        -D publish correlation-data "$ID" \
        -t "system/$NAME/$1" \
        -m "{\"at\":\"$(date -u +%FT%TZ)\",\"e\":$2}"
    }

    while true; do
      ID=$(cat /proc/sys/kernel/random/uuid 2>/dev/null || uuidgen)
      load=$(( $(statgrab -u -f 100 load.min5) / $(statgrab -u general.ncpus) )) # % of cores
      mem=$(( ($(statgrab -u mem.free) + $(statgrab -u mem.cache)) * 100 / $(statgrab -u mem.total) ))
      disk=$(df --output=pcent / | tail -1 | tr -d ' ')
      stats="{\"loadPct\":$load,\"memAvailPct\":$mem,\"diskUsed\":\"$disk\"}"

      pub start "{\"label\":\"$NAME\",\"rules\":{\"timeout\":60,\"expected\":300,\"critical\":false},\"content\":$stats}" &&
      pub finished '{"summary":"","success":true}'
      sleep 300
    done
  '';
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
            inherit (cfg) enable proxyTo listenOn nodeId bridge2;
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
      environment.systemPackages = lib.optional (cfg.package != null) cfg.package;

      assertions = [ (mosquittoAssertion lib mosquitto.package) ];

      # launchd chdirs into WorkingDirectory, so it must already exist
      system.activationScripts.preActivation.text = ''
        mkdir -p ${mosquitto.dataDir}
      '';

      # mosquitto drops privileges to this user on launch
      # (its built-in `user` option defaults to "mosquitto")
      users.knownUsers = [ "mosquitto" ];
      users.knownGroups = [ "mosquitto" ];
      users.users.mosquitto = {
        uid = 4435;
        gid = 4435;
        home = mosquitto.dataDir; # the daemon makes sure this is owned by mosquitto
        description = "Mosquitto MQTT Broker user";
        isHidden = true;
      };
      users.groups.mosquitto.gid = 4435;

      launchd.daemons.mosquitto = {
        # Execute the exact same shell preStart and ExecStart commands as a
        # nixos system, but the pkgs referenced will be darwin pkgs!
        script = ''
          export PATH=${pkgs.coreutils}/bin:$PATH # for `install`
          ${mosquitto_unit.preStart}
          # preStart runs as root, but mosquitto (service) drops
          # privileges to the mosquitto (user)
          chown -R mosquitto:mosquitto ${mosquitto.dataDir}
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

      launchd.daemons.control-events-system-healthcheck = lib.mkIf cfg.system-healthcheck {
        serviceConfig = {
          ProgramArguments = [ "${healthcheckLoop pkgs mosquitto.package cfg.nodeId}" ];
          RunAtLoad = true;
          KeepAlive = true;
        };
      };
    };
  };

  flake.nixosModules.control-events = { config, lib, pkgs, ... }:
  let cfg = config.services.control-events-node; in
  {

    imports = [ nodeOptions ];

    config = lib.mkIf cfg.enable {
      environment.systemPackages = lib.optional (cfg.package != null) cfg.package;

      assertions = [ (mosquittoAssertion lib config.services.mosquitto.package) ];

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
        # In our case, we always proxy topics out, and optionally in.
        bridges = lib.mkIf (cfg.proxyTo != null) {
          "proxy_to" = {
            addresses = [{ address = cfg.proxyTo; }];
            topics = [ "# out 2" ] ++ lib.optional cfg.bridge2 "# in 2";
            settings = {
              cleansession = false;
              # 0 (default) would make the session expire immediately when the
              # connection drops, rather than queueing messages
              bridge_session_expiry_interval = 86400*5; # 5 days
              remote_clientid = cfg.nodeId;
              bridge_protocol_version = "mqttv50";
            };
          };
        };

        # Persist messages with the sqlite plugin, which commits changes every
        # few seconds (its flush_period, 5s), rather than the built-in
        # persistence, as suggested in the manpage.
        persistence = false; # can't be on with a persistence plugin
        includeDirs = [
          (pkgs.writeTextDir "persist-sqlite.conf" ''
            global_plugin ${config.services.mosquitto.package.lib}/lib/mosquitto_persist_sqlite.so
            plugin_opt_db_file ${config.services.mosquitto.dataDir}/mosquitto.sqlite3
          '')
        ];
        settings = {
          max_queued_messages = 5000; # keep more than the 1000 default
        };
      };

      networking.firewall = lib.mkIf (cfg.listenOn != null) {
        # node is accessible from other machines, but mosquitto should only be
        # bound on private addr in `listenOn` (1883 is the default port)
        # WARNING: If you add a listener above with a public IP, anyone can use the broker.
        allowedTCPPorts = [ 1883 ];
      };

      systemd.services.control-events-system-healthcheck = lib.mkIf cfg.system-healthcheck {
        description = "Send control-events system healthchecks";
        wantedBy = [ "multi-user.target" ];
        after = [ "mosquitto.service" ];
        serviceConfig = {
          ExecStart = healthcheckLoop pkgs config.services.mosquitto.package cfg.nodeId;
          Restart = "always";
          DynamicUser = true;
        };
      };

     };

  };
}
