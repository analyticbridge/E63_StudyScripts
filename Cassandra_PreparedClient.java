package edu.hu.cassandra;

	import java.util.HashSet;
import java.util.Set;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
	
	public class PreparedClient {
	   private Cluster cluster;
	   private Session session;

	   public void connect(String node) {
	      cluster = Cluster.builder()
	            .addContactPoint(node).build();
	      session = cluster.connect("mykeyspace");
	      Metadata metadata = cluster.getMetadata();
	      System.out.printf("Connected to cluster: %s\n", 
	            metadata.getClusterName());
	      for ( Host host : metadata.getAllHosts() ) {
	         System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n",
	               host.getDatacenter(), host.getAddress(), host.getRack());
	      }
	   }

	   public void createSchema() {
		   session.execute("CREATE KEYSPACE mykeyspaceprob3 WITH replication " + 
				      "= {'class':'SimpleStrategy', 'replication_factor':1};");
		   session.execute(
				      "CREATE TABLE mykeyspaceprob3.person (" +
				            "user_id int PRIMARY KEY," + 
				            "fname text," + 
				            "lname text," + 
				            "city text," + 
				            "mobile set<text>," + 
				            ");");

	   }
	   public void loadData() {
		   
		   PreparedStatement statement = session.prepare(
				      "INSERT INTO mykeyspaceprob3.person " +
				      "(user_id, fname, lname, city, mobile) " +
				      "VALUES (?, ?, ?, ?, ?);");
		   
		   BoundStatement boundStatement = new BoundStatement(statement);
		   
		   Set<String> mobile = new HashSet<String>();
		   mobile.add("00167668334"); mobile.add("00167668345645"); mobile.add("001676687757");		   
		   session.execute(boundStatement.bind(
		         1,
		         "john",
		         "smith",
		         "Rockhampton",
		         mobile ) );
		   //clearing the mobile HashSet and setting the new values
		   mobile.clear(); mobile.add("00167645345334");mobile.add("00163453687757");
		   session.execute(boundStatement.bind(
			         2,
			         "peter",
			         "gabriel",
			         "Los Angeles",
			         mobile ) );
		 //clearing the mobile HashSet and setting the new values
		   mobile.clear(); mobile.add("00167611345334");mobile.add("001478345645");mobile.add("00173453687757");
		   session.execute(boundStatement.bind(
			         3,
			         "david",
			         "bowie",
			         "Detroit",
			         mobile ) );


	   }
	   public void querySchema(){
		   ResultSet results = session.execute("SELECT user_id, fname, lname, city, mobile FROM mykeyspaceprob3.person " +
			        "WHERE user_id = 2;");
		   System.out.println(String.format("%-20s\t%-20s\t%-20s\t%-20s\t%-20s\n%s", "user_id", "fname", "lname", "city", "mobile",
			    	  "---------------------+-----------------------+--------------------+--------------------+--------------------"));
		   for (Row row : results) {
			    System.out.println(String.format("%-20s\t%-20s\t%-20s\t%-20s\t%-20s", row.getInt("user_id"),
			    row.getString("fname"),  row.getString("lname"), row.getString("city"), row.getSet("mobile",String.class)));
			}
			System.out.println();


	   }
	   
	   public void close() {
	      cluster.close(); // .shutdown();
	   }

	   public static void main(String[] args) {
	      PreparedClient client = new PreparedClient();
	      client.connect("127.0.0.1");
	      client.createSchema();
           client.loadData();
	      client.querySchema();
	      client.close();
	   }
	}
