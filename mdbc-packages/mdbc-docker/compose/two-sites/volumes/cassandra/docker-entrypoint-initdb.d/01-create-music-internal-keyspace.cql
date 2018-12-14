CREATE KEYSPACE IF NOT EXISTS music_internal WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} AND durable_writes = true;
