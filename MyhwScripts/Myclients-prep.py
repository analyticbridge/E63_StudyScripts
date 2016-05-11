#!/usr/bin/env python
# -*- coding: utf-8 -*-

from cassandra.cluster import Cluster
import logging
import time

log = logging.getLogger()
log.setLevel('INFO')

class SimpleClient(object):
    session = None

    def connect(self, nodes):
        cluster = Cluster(nodes)
        metadata = cluster.metadata
        self.session = cluster.connect()
        log.info('Connected to cluster: ' + metadata.cluster_name)
        for host in metadata.all_hosts():
            log.info('Datacenter: %s; Host: %s; Rack: %s',
                host.datacenter, host.address, host.rack)

    def close(self):
        self.session.cluster.shutdown()
        log.info('Connection closed.')


    def create_schema(self):
        self.session.execute("""CREATE KEYSPACE simplex WITH replication = {'class':'SimpleStrategy', 'replication_factor':3};""")
        self.session.execute("""
            CREATE TABLE simplex.songs (
                id uuid PRIMARY KEY,
                title text,
                album text,
                artist text,
                tags set<text>,
                data blob
            );
        """)
        self.session.execute("""
            CREATE TABLE simplex.playlists (
                id uuid,
                title text,
                album text,
                artist text,
                song_id uuid,
                PRIMARY KEY (id, title, album, artist)
            );
        """)
        log.info('Simplex keyspace and schema created.')


    def load_data(self):
        self.session.execute("""
            INSERT INTO simplex.songs (id, title, album, artist, tags)
            VALUES (
                756716f7-2e54-4715-9f00-91dcbea6cf50,
                'La Petite Tonkinoise',
                'Bye Bye Blackbird',
                'Joséphine Baker',
                {'jazz', '2013'}
            );
        """)
        self.session.execute(bound_statement,
			[ UUID("2cc9ccb7-6221-4ccb-8387-f22b6a1b354d"),
			UUID("756716f7-2e54-4715-9f00-91dcbea6cf50"),
			"La Petite Tonkinoise",
			"Bye Bye Blackbird",
			"Joséphine Baker" ]
			)
		
            );
        """)
        log.info('Data loaded.')
    
    def query_schema(self):
        results = self.session.execute("""
    SELECT * FROM simplex.playlists
    WHERE id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d;
""")
        print "%-30s\t%-20s\t%-20s\n%s" % \
    ("title", "album", "artist",
        "-------------------------------+-----------------------+--------------------")
        for row in results:
            print "%-30s\t%-20s\t%-20s" % (row.title, row.album, row.artist)
        log.info('Schema queried.')

def main():
    logging.basicConfig()
    client = BoundStatementsClient()
    client.connect(['127.0.0.1'])
    client.create_schema()
    client.prepare_statements()
    client.load_data()
    client.query_schema()
    client.update_schema()
    client.drop_schema("simplex")
    client.close()
	

if __name__ == "__main__":
    main()