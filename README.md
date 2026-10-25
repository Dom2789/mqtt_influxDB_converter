# mqtt_influxDB_converter

Subscribes to MQTT topics and writes the received values into an InfluxDB bucket.
Runs as a long-lived systemd service on a Linux host.

> Status: early development. `main.py` is still a stub loop; the deployment
> scaffolding (config, env file, systemd unit) is in place and described below.

## Requirements

- Linux host with systemd
- Python >= 3.12 (see `.python-version`)
- [uv](https://docs.astral.sh/uv/) for dependency management
- A reachable InfluxDB v2 instance and an MQTT broker

## Repository layout

| Path                              | Purpose |
|-----------------------------------| --- |
| `main.py`                         | Entry point started by the systemd unit |
| `src/_lib/ConfigToml.py`          | Read-only `Mapping` wrapper around a TOML config file |
| `src/_lib/logger.py`              | `setup_logging()` — file logging, optional date suffix, debug level |
| `config.toml`                     | Application configuration (non-secret) |
| `influx.env.example`              | Template for the InfluxDB credentials, deployed to `/etc/automation/influx.env` |
| `mqtt_influxDB_converter.service` | systemd unit |
| `pyproject.toml` / `uv.lock`      | Project metadata and locked dependencies |

## Deployment

The paths below are the ones hard-coded in `mqtt_influxDB_converter.service`.
If you deploy somewhere else, edit `WorkingDirectory`, `ExecStart`,
`EnvironmentFile` and `ReadWritePaths` to match.

### 1. Get the code onto the host

```bash
git clone <repo-url> /root/mqtt_influxDB_converter
cd /root/mqtt_influxDB_converter
```

### 2. Create the virtual environment

The unit runs the interpreter from the project-local `.venv`, so it has to
exist at `/root/mqtt_influxDB_converter/.venv`:

```bash
uv sync
```

Verify it:

```bash
/root/mqtt_influxDB_converter/.venv/bin/python --version
```

### 3. Provide the InfluxDB credentials

The unit loads its secrets from `/etc/automation/influx.env` — outside the
repository, so tokens are never committed. Use `influx.env.example` as the template:

```bash
install -d -m 0750 /etc/automation
install -m 0600 influx.env.example /etc/automation/influx.env.example
$EDITOR /etc/automation/influx.env.example
```

Keys expected by the service:

| Variable | Description | Example |
| --- | --- | --- |
| `INFLUX_URL` | Base URL of the InfluxDB instance | `http://192.168.1.10:8086` |
| `INFLUX_TOKEN` | API token with write access to the bucket | `abc123...` |
| `INFLUX_ORG` | InfluxDB organisation | `homelab` |
| `INFLUX_BUCKET` | Target bucket | `telemetry` |

Format is systemd `EnvironmentFile` syntax: one `KEY=value` per line, no
`export`, no quoting unless the value really contains the quotes.

### 4. Create the log directory

`setup_logging()` writes to a file, and the unit runs with
`ProtectSystem=strict`, so the only writable location is the one listed in
`ReadWritePaths`:

```bash
install -d -m 0755 /root/logs
```

### 5. Install and start the unit

```bash
cp mqtt_influxDB_converter.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now mqtt_influxDB_converter.service
```

### 6. Check that it is running

```bash
systemctl status mqtt_influxDB_converter.service
journalctl -u mqtt_influxDB_converter.service -f
tail -f /root/logs/*.log
```

## The systemd unit explained

```ini
[Unit]
After=network-online.target
Wants=network-online.target
```

Startup is delayed until the network is actually up, since the service needs
both the broker and InfluxDB.

```ini
[Service]
User=root
Group=root
WorkingDirectory=/root/mqtt_influxDB_converter
ExecStart=/root/mqtt_influxDB_converter/.venv/bin/python /root/mqtt_influxDB_converter/main.py
EnvironmentFile=/etc/automation/influx.env
```

The venv interpreter is invoked directly, so no shell or activation step is
involved. `EnvironmentFile` injects the InfluxDB variables into the process
environment.

Hardening:

| Directive | Effect |
| --- | --- |
| `NoNewPrivileges=true` | The process cannot gain privileges via setuid/setgid |
| `ProtectSystem=strict` | The whole filesystem is read-only except `ReadWritePaths` |
| `ReadWritePaths=/root/logs/` | The one writable path — the log destination |
| `PrivateTmp=true` | Private `/tmp` and `/var/tmp` |

Restart behaviour:

| Directive | Effect |
| --- | --- |
| `Restart=always` | Restarted on any exit, clean or not |
| `RestartSec=5` | Five seconds between restart attempts |

## Updating a deployment

```bash
cd /root/mqtt_influxDB_converter
git pull
uv sync                                  # only needed if dependencies changed
systemctl restart mqtt_influxDB_converter.service
```

If the unit file itself changed, copy it over and `systemctl daemon-reload`
before restarting.

## Uninstalling

```bash
systemctl disable --now mqtt_influxDB_converter.service
rm /etc/systemd/system/mqtt_influxDB_converter.service
systemctl daemon-reload
rm -rf /root/mqtt_influxDB_converter
rm /etc/automation/influx.env.example            # removes the credentials
```

## Troubleshooting

| Symptom | Likely cause |
| --- | --- |
| `status=203/EXEC` | `.venv` missing or `ExecStart` path wrong — run `uv sync` in `WorkingDirectory` |
| `Failed to load environment files` | `/etc/automation/influx.env` missing or unreadable by root |
| `PermissionError` on the log file | Log directory missing, or its path is not covered by `ReadWritePaths` |
| Service restarts in a loop every 5 s | Crash on startup; read the traceback with `journalctl -u mqtt_influxDB_converter.service` |
| Config or token changes have no effect | The process reads them at startup only — restart the service |

## Local development

```bash
uv sync
uv run main.py
```

For a local run, export the `INFLUX_*` variables in your shell (or source the
env file) instead of relying on systemd:

```bash
set -a && . ./influx.env.example && set +a
uv run main.py
```

## License

MIT — see [LICENSE](LICENSE).
