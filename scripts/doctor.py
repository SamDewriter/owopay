"""make doctor: can this machine run the course stack? Standard library only, so it runs before setup."""

import json
import os
import platform
import shutil
import socket
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PORTS = {
    5432: 'Postgres',
    9092: 'Redpanda',
    8081: 'Schema Registry',
    8082: 'Redpanda Console',
    8333: 'SeaweedFS S3',
    8888: 'SeaweedFS filer',
    9333: 'SeaweedFS master',
    8080: 'Airflow',
    8084: 'Spark master UI',
    3000: 'Metabase',
    9090: 'Prometheus',
    3001: 'Grafana',
}
failed = 0


def line(ok, name, detail, hint=''):
    global failed
    failed += ok is False
    print('  %-4s  %-18s %s' % ({True: 'PASS', False: 'FAIL', None: 'WARN'}[ok], name, detail))
    if hint and ok is not True:
        print('        hint: %s' % hint)


def run(*cmd):
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        return out.returncode, (out.stdout + out.stderr).strip()
    except (OSError, subprocess.TimeoutExpired) as e:
        return 1, str(e)


def env_value(key, default):
    for name in ('.env', '.env.example'):
        path = ROOT / name
        if path.exists():
            for ln in path.read_text().splitlines():
                if ln.startswith(key + '='):
                    return ln.split('=', 1)[1].strip()
    return default


def other_listeners(port):
    """Programs other than Docker listening on a port, via lsof; None when lsof is unavailable."""
    if not shutil.which('lsof'):
        return None
    _, out = run('lsof', '-nP', '-iTCP:%d' % port, '-sTCP:LISTEN')
    names = {ln.split()[0] for ln in out.splitlines()[1:] if ln.strip()}
    return sorted(n for n in names if not n.startswith(('com.docke', 'docker', 'vpnkit', 'Docker')))


def total_ram_gb():
    if hasattr(os, 'sysconf') and 'SC_PHYS_PAGES' in os.sysconf_names:
        return os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES') / 1024**3
    return None


def main():
    print('make doctor · %s %s, Python %s' % (platform.system(), platform.machine(), platform.python_version()))

    ram = total_ram_gb()
    if ram is None:
        line(None, 'RAM', 'could not read')
    else:
        line(ram >= 7.5 or None, 'RAM', '%.1f GB' % ram, 'under 8 GB: use a Codespace')
    line(None if (os.cpu_count() or 0) < 4 else True, 'CPU cores', os.cpu_count(), '4 cores recommended')
    free = shutil.disk_usage(ROOT).free / 1024**3
    line(free >= 20 or (False if free < 12 else None), 'Free disk', '%.0f GB' % free, 'images ~6 GB + data ~3 GB')

    if 'microsoft' in platform.release().lower():
        line(True, 'WSL2', platform.release())
        if str(ROOT).startswith('/mnt/'):
            line(False, 'Repo location', str(ROOT), 'clone inside the WSL filesystem (~/), not under /mnt/c')
    elif platform.system() == 'Windows':
        line(False, 'WSL2', 'running on Windows directly', 'use WSL2 with Docker Desktop, or a Codespace')

    code, out = run('docker', 'info', '--format', '{{.MemTotal}} {{.ServerVersion}}')
    if code:
        line(False, 'Docker', 'not reachable', 'start Docker Desktop (or the docker service)')
    else:
        mem, version = out.split()[:2]
        mem_gb = int(mem) / 1024**3
        line(True, 'Docker', 'engine %s' % version)
        line(
            mem_gb >= 5.5 or None,
            'Docker memory',
            '%.1f GB' % mem_gb,
            'Docker Desktop > Settings > Resources: give it 6 GB on an 8 GB laptop',
        )
    code, out = run('docker', 'compose', 'version', '--short')
    line(code == 0 or False, 'Compose v2', out if code == 0 else 'missing', 'install the Compose v2 plugin')

    ours = set()
    if code == 0:
        _, ps = run('docker', 'compose', '--project-directory', str(ROOT), '--profile', '*', 'ps', '--format', 'json')
        for row in (json.loads(r) for r in ps.splitlines() if r.startswith('{')):
            ours |= {p['PublishedPort'] for p in row.get('Publishers') or [] if p.get('PublishedPort')}
    ports = dict(PORTS)
    ports[int(env_value('PG_PORT', '5432'))] = ports.pop(5432)
    clashes = []
    for port, name in ports.items():
        others = other_listeners(port)
        if others is None:  # no lsof: fall back to "something answers and it is not our container"
            with socket.socket() as s:
                s.settimeout(0.3)
                others = ['unknown'] if port not in ours and s.connect_ex(('127.0.0.1', port)) == 0 else []
        if others:
            clashes.append('%d %s (held by %s)' % (port, name, ', '.join(others)))
    line(
        not clashes or False,
        'Ports',
        'free or ours' if not clashes else '; '.join(clashes),
        'stop that program, or move our port in .env (e.g. PG_PORT=5433 if you keep a local Postgres)',
    )

    images = [
        v
        for k, v in (
            ln.split('=', 1)
            for ln in (ROOT / 'versions.env').read_text().splitlines()
            if '=' in ln and not ln.startswith('#')
        )
    ]
    missing = [img for img in images if run('docker', 'image', 'inspect', img)[0]]
    line(
        None if missing else True,
        'Images pulled',
        '%d of %d' % (len(images) - len(missing), len(images)),
        'pull them on a good connection: docker compose --profile "*" pull',
    )

    print('\nmake doctor: %s' % ('all PASS' if not failed else '%d FAIL' % failed))
    raise SystemExit(1 if failed else 0)


if __name__ == '__main__':
    main()
