"""Settings shared by the generator and the scripts: .env.example, then .env, then the real environment."""

import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def _read_env_file(path):
    values = {}
    if not path.exists():
        return values
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        key, value = line.split('=', 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def load_settings():
    settings = _read_env_file(REPO_ROOT / '.env.example')
    settings.update(_read_env_file(REPO_ROOT / '.env'))
    settings.update({k: v for k, v in os.environ.items() if k in settings})
    return settings


SETTINGS = load_settings()


def pg_conninfo(db=None):
    s = SETTINGS
    return dict(
        host=s['PG_HOST'],
        port=int(s['PG_PORT']),
        dbname=db or s['PG_DB'],
        user=s['PG_USER'],
        password=s['PG_PASSWORD'],
        connect_timeout=5,
    )


def s3_client():
    import boto3
    from botocore.config import Config

    s = SETTINGS
    return boto3.client(
        's3',
        endpoint_url=s['S3_ENDPOINT'],
        aws_access_key_id=s['S3_ACCESS_KEY'],
        aws_secret_access_key=s['S3_SECRET_KEY'],
        region_name=s['S3_REGION'],
        config=Config(s3={'addressing_style': 'path'}),
    )
