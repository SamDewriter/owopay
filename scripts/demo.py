"""make demo: what is in the three systems right now."""

from common import pg, s3_client, topic_sizes


def main():
    print('Postgres · owopay')
    with pg() as conn:
        for table in ['customers', 'merchants', 'agents', 'wallets', 'transactions']:
            n = conn.execute('select count(*) from %s' % table).fetchone()[0]
            print('  %-14s %12s rows' % (table, format(n, ',')))
        days = conn.execute("""select (created_at at time zone 'Africa/Lagos')::date, count(*)
                               from transactions group by 1 order by 1""").fetchall()
        for day, n in days:
            print('    %s   %12s' % (day, format(n, ',')))

    print('\nRedpanda topics')
    for topic, n in sorted(topic_sizes().items()):
        print('  %-18s %10s events' % (topic, format(n, ',')))

    print('\nSeaweedFS buckets')
    s3 = s3_client()
    for b in s3.list_buckets().get('Buckets', []):
        keys = s3.list_objects_v2(Bucket=b['Name']).get('KeyCount', 0)
        print('  %-18s %10d objects' % (b['Name'], keys))


if __name__ == '__main__':
    main()
