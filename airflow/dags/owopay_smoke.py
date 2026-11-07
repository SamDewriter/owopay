"""owopay_smoke: proves Airflow can reach the rest of the stack. Trigger it once after make up PROFILE=orchestration."""

import os
import socket

import pendulum
from airflow.sdk import dag, task

TARGETS = {
    'postgres': (os.environ.get('OWOPAY_PG_HOST', 'postgres'), 5432),
    'redpanda': tuple(os.environ.get('OWOPAY_KAFKA_BOOTSTRAP', 'redpanda:29092').rsplit(':', 1)),
    'seaweedfs': ('seaweedfs', 8333),
}


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 11, 1, tz='Africa/Lagos'),
    catchup=False,
    tags=['owopay', 'week4'],
)
def owopay_smoke():
    @task
    def reach(name: str):
        host, port = TARGETS[name]
        with socket.create_connection((host, int(port)), timeout=5):
            return '%s reachable at %s:%s' % (name, host, port)

    for name in TARGETS:
        reach.override(task_id='reach_%s' % name)(name)


owopay_smoke()
