package edu.hu.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
// import com.datastax.driver.core.Session;

public class SimpleClient {
   private Cluster cluster;
   // private Session session;

   public void connect(String node) {
      cluster = Cluster.builder()
            .addContactPoint(node).build();
      
      // session = cluster.connect("mykeyspace");
      Metadata metadata = cluster.getMetadata();
      System.out.printf("Connected to cluster: %s\n", 
            metadata.getClusterName());
      for ( Host host : metadata.getAllHosts() ) {
         System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n",
               host.getDatacenter(), host.getAddress(), host.getRack());
      }
   }

   public void close() {
      cluster.close(); // .shutdown();
   }

   public static void main(String[] args) {
      SimpleClient client = new SimpleClient();
      client.connect("127.0.0.1");
      client.close();
   }
}
