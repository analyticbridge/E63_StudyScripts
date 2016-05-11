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
        self.session.execute("CREATE KEYSPACE pyperson WITH replication = {'class':'SimpleStrategy', 'replication_factor':3};")
        self.session.execute("
            CREATE TABLE pyperson.person 
                id pid PRIMARY KEY,
                fname text,
                lname text,
                city text,
                mobile int;
                ")
        
        log.info('pyperson keyspace and schema created.')
	

    def load_data(self):
        self.session.execute("
            INSERT INTO person(id, fname, lname, city, mobile)
            VALUES (
                101,
                'gregoryLa',
                'Dave',
                'cambridge',
                {'888888888'}
            );
        ")
 
        log.info('Data loaded.')
    
    def query_schema(self):
        results = self.session.execute("
    SELECT * FROM simplex.playlists
    WHERE id = 101;
        ")
        print "id" % \
    ("fname", "lname", "city", "mobile"
        "-------------------------------+-----------------------+--------------------")
        for row in results:
            print "id" % (row.fname, row.lname, row.city, row.mobile)
        log.info('Schema queried.')

def main():
    logging.basicConfig()
    client = SimpleClient()
    client.connect(['127.0.0.1'])
    client.create_schema()
    time.sleep(10)
    client.load_data()
    client.query_schema()
    client.close()

if __name__ == "__main__":
    main()