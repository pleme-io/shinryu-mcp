# Shinryū home-manager module — daemon (refiner + materializer) + MCP server entry
#
# Namespace: services.shinryu.daemon.* / services.shinryu.mcp.*
#
# The daemon runs shinryu-mcp in daemon mode with inotify-driven refiner
# and channel-driven materializer. Bronze NDJSON is read from the analytics
# path (shared with Vector via PVC in K8s, or a local directory on macOS).
#
# Module factory: receives { hmHelpers } from flake.nix, returns HM module.
{ hmHelpers }:
{
  lib,
  config,
  pkgs,
  ...
}:
with lib; let
  inherit (hmHelpers) mkMcpOptions mkMcpServerEntry mkLaunchdService mkSystemdService;
  daemonCfg = config.services.shinryu.daemon;
  mcpCfg = config.services.shinryu.mcp;
  isDarwin = pkgs.stdenv.isDarwin;

  analyticsPath = daemonCfg.analyticsPath;

  logDir = if isDarwin
    then "${config.home.homeDirectory}/Library/Logs"
    else "${config.home.homeDirectory}/.local/share/shinryu/logs";
in {
  options.services.shinryu = {
    # ── Daemon options ─────────────────────────────────────────────────
    daemon = {
      enable = mkOption {
        type = types.bool;
        default = false;
        description = "Enable Shinryū daemon (refiner + materializer for Bronze→Silver→Gold)";
      };

      analyticsPath = mkOption {
        type = types.str;
        default = "${config.home.homeDirectory}/.local/share/shinryu/analytics";
        description = "Root path for Bronze/Silver/Gold data tiers. On K8s this is the shared PVC mount.";
      };

      datafusion = {
        memoryLimitMb = mkOption {
          type = types.int;
          default = 2048;
          description = "DataFusion memory limit in MB";
        };

        sqlTimeoutSecs = mkOption {
          type = types.int;
          default = 300;
          description = "SQL query timeout in seconds";
        };
      };

      refiner = {
        enable = mkOption {
          type = types.bool;
          default = true;
          description = "Enable inotify-driven Bronze→Silver refiner";
        };

        intervalSecs = mkOption {
          type = types.int;
          default = 60;
          description = "Refiner poll interval (fallback if inotify unavailable)";
        };
      };

      materializer = {
        enable = mkOption {
          type = types.bool;
          default = true;
          description = "Enable channel-driven Silver→Gold materializer";
        };

        intervalSecs = mkOption {
          type = types.int;
          default = 300;
          description = "Materializer interval in seconds";
        };
      };

      udfs = {
        burstForge = mkOption {
          type = types.bool;
          default = true;
          description = "Enable burst-forge domain UDFs (tumbling_window, asof_nearest)";
        };
      };
    };

    # ── MCP options (from substrate hm-service-helpers) ───────────────
    mcp = mkMcpOptions {
      defaultPackage = pkgs.shinryu-mcp or null;
    };
  };

  # ── Config ─────────────────────────────────────────────────────────
  config = mkMerge [
    # MCP server entry — stdio mode, points at local analytics path
    (mkIf mcpCfg.enable {
      services.shinryu.mcp.serverEntry = mkMcpServerEntry {
        command = "${mcpCfg.package}/bin/shinryu-mcp";
        args = [ "--analytics-path" analyticsPath ];
      };
    })

    # Ensure analytics directory exists
    (mkIf (daemonCfg.enable || mcpCfg.enable) {
      home.activation.shinryu-analytics-dir = lib.hm.dag.entryAfter ["writeBoundary"] ''
        run mkdir -p "${analyticsPath}"
        run mkdir -p "${analyticsPath}/bronze"
        run mkdir -p "${analyticsPath}/silver"
        run mkdir -p "${analyticsPath}/gold"
      '';
    })

    # Darwin: launchd agent for shinryu daemon mode
    (mkIf (daemonCfg.enable && isDarwin)
      (mkLaunchdService {
        name = "shinryu-daemon";
        label = "io.pleme.shinryu-daemon";
        command = "${mcpCfg.package}/bin/shinryu-mcp";
        args = [
          "--daemon"
          "--analytics-path" analyticsPath
        ];
        logDir = logDir;
      }))

    # Linux: systemd service for shinryu daemon mode
    (mkIf (daemonCfg.enable && !isDarwin)
      (mkSystemdService {
        name = "shinryu-daemon";
        description = "Shinryū daemon — DataFusion analytical query plane";
        command = "${mcpCfg.package}/bin/shinryu-mcp";
        args = [
          "--daemon"
          "--analytics-path" analyticsPath
        ];
      }))
  ];
}
