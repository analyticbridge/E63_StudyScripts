package org.neo4j.graphproject;

import java.net.URI;
import java.net.URISyntaxException;

import javax.ws.rs.core.MediaType;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class ConnectToServer
{
    private static final String SERVER_ROOT_URI = "http://localhost:7474/db/data/";

    public static void main( String[] args ) throws URISyntaxException
    {
          checkDatabaseIsRunning();
          

          
          
          String createJWMovie = "CREATE (johnWick:Movie { title : 'John Wick', year : '2001' })" 
        		  + " CREATE (william:Actor { name:'William Dafoe' })" 
        		  + " CREATE (michael:Actor { name:'Michael Nyquist' })"
        		  + " CREATE (chad:Director { name:'Chad Stahelski' })"
        		  + " CREATE (david:Director { name:'David Leitch' })" ; 
        		  
          sendTransactionalCypherQuery(createJWMovie) ; 
          
          
          String createRelationship1 = "MATCH (keanu:Actor { name : 'Keanu Reeves' }) MATCH(johnWick:Movie { title: 'John Wick'}) CREATE (keanu)-[r:ACTS_IN {role: 'Fighter'}]->(johnWick)" ; 
          sendTransactionalCypherQuery(createRelationship1) ;
          String createRelationship2 = "MATCH (william:Actor { name : 'William Dafoe' }) MATCH(johnWick:Movie { title: 'John Wick'}) CREATE (william)-[r:ACTS_IN {role: 'Comedian'}]->(johnWick)" ; 
          sendTransactionalCypherQuery(createRelationship2) ;
          String createRelationship3 = "MATCH (michael:Actor { name : 'Michael Nyquist' }) MATCH(johnWick:Movie { title: 'John Wick'}) CREATE (michael)-[r:ACTS_IN {role: 'Buttler'}]->(johnWick)" ; 
          sendTransactionalCypherQuery(createRelationship3) ;
          String createRelationship4 = "MATCH (chad:Director { name : 'Chad Stahelski' }) MATCH(johnWick:Movie { title: 'John Wick'}) CREATE (chad)-[r:DIRECTS {role: 'Director'}]->(johnWick)" ; 
          sendTransactionalCypherQuery(createRelationship4) ;
          String createRelationship5 = "MATCH (david:Director { name : 'David Leitch' }) MATCH(johnWick:Movie { title: 'John Wick'}) CREATE (david)-[r:DIRECTS {role: 'Director'}]->(johnWick)" ; 
          sendTransactionalCypherQuery(createRelationship5) ;
  
           sendTransactionalCypherQuery( "MATCH (n) RETURN n" );
    }

    private static void sendTransactionalCypherQuery(String query) {
        // START SNIPPET: queryAllNodes
        final String txUri = SERVER_ROOT_URI + "transaction/commit";
        WebResource resource = Client.create().resource( txUri );

        String payload = "{\"statements\" : [ {\"statement\" : \"" +query + "\"} ]}";
        ClientResponse response = resource
                .accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON )
                .entity( payload )
                .post( ClientResponse.class );
        
        System.out.println( String.format(
                "POST [%s] to [%s], status code [%d], returned data: "
                        + System.lineSeparator() + "%s",
                payload, txUri, response.getStatus(),
                response.getEntity( String.class ) ) );
        
        response.close();
        // END SNIPPET: queryAllNodes
    }

    

    private static void checkDatabaseIsRunning()
    {
        // START SNIPPET: checkServer
        WebResource resource = Client.create()
                .resource( SERVER_ROOT_URI );
        ClientResponse response = resource.get( ClientResponse.class );

        System.out.println( String.format( "GET on [%s], status code [%d]",
                SERVER_ROOT_URI, response.getStatus() ) );
        response.close();
        // END SNIPPET: checkServer
    }
}
