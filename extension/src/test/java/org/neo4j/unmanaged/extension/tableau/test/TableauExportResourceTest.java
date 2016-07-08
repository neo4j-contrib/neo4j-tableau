package org.neo4j.unmanaged.extension.tableau.test;

import org.neo4j.unmanaged.extension.tableau.TableauExportResource;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import javax.ws.rs.core.MediaType;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import org.neo4j.harness.ServerControls;
import org.neo4j.test.server.HTTP;

import static org.junit.Assert.*;
import org.junit.Rule;
import org.neo4j.harness.junit.Neo4jRule;

public class TableauExportResourceTest {
    private ServerControls server;
    
    private static final String CYPHER_STATEMENT =
            new StringBuilder()
                    .append("CREATE (user1:User {user_id:'u1', name:'Max', age: 27})")
                    .append("CREATE (friend1:User {user_id:'f1', name:'Michael', age: 43.7})")
                    .append("CREATE (friend2:User {user_id:'f2', name:'Peter', married: true})")
                    .append("CREATE (friend3:User {user_id:'f3', name:'David', married: null})")
                    .append("CREATE (friend4:User {user_id:'f4', name:'Horst', married: false})")
                    .append("CREATE (user1)-[:FRIENDS]->(friend1)")
                    .append("CREATE (user1)-[:FRIENDS]->(friend2)")
                    .append("CREATE (user1)-[:FRIENDS]->(friend3)")
                    .toString();

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
        .withExtension( "/test", TableauExportResource.class )
//        .withFixture(CYPHER_STATEMENT)
        .withFixture(new File("src/test/resources/movie_graph.cyp"))
        ;

    //@Test
    public void testTde() throws Exception
    {
        // Given
        URI serverURI = neo4j.httpURI();
        
//        String cypherQuery = "match (n) return n";
//        String cypherQuery = "Match (n:Movie) return n";
//        String cypherQuery = "Match (n:Movie) return n.Title"; // --> UNKNOWN
//        String cypherQuery = "Match (n:Movie) return n.title";
        String cypherQuery = "Match (p:Person)-[a:ACTED_IN]->(m:Movie) return p,a,m";

        String queryEncoded = URLEncoder.encode(cypherQuery, "UTF-8")
                                            .replaceAll("\\+", "%20")
                                            .replaceAll("\\%21", "!")
                                            .replaceAll("\\%27", "'")
                                            .replaceAll("\\%28", "(")
                                            .replaceAll("\\%29", ")")
                                            .replaceAll("\\%7E", "~");
        System.out.println(cypherQuery);
        // When I access the server
        HTTP.Response response = HTTP.withHeaders("Content-Type", "application/x-download")
                        .GET( serverURI.resolve( "/test/tableau/tde/" + queryEncoded).toString() );

        System.out.println(response.toString());
        // Then it should reply
        assertEquals(200, response.status());
    }

    //@Test
    public void testJson() throws Exception
    {
        // Given
        URI serverURI = neo4j.httpURI();
        
//        String cypherQuery = "match (n) return n";
//        String cypherQuery = "Match (n:Movie) return n";
//        String cypherQuery = "Match (n:Movie) return n.Title"; // --> UNKNOWN
//        String cypherQuery = "Match (n:Movie) return n.title";
//        String cypherQuery = "Match (p:Person)-[a:ACTED_IN]->(m:Movie) return p,a,m";
        String cypherQuery = "Match (p:Person)-[a:ACTED_IN]->(m:Movie) return p.name,a.roles,m.title";

        String queryEncoded = URLEncoder.encode(cypherQuery, "UTF-8")
                                            .replaceAll("\\+", "%20")
                                            .replaceAll("\\%21", "!")
                                            .replaceAll("\\%27", "'")
                                            .replaceAll("\\%28", "(")
                                            .replaceAll("\\%29", ")")
                                            .replaceAll("\\%7E", "~");
        System.out.println(cypherQuery);

        // When I access the server
        HTTP.Response response = HTTP.withHeaders("Accept", MediaType.TEXT_PLAIN)
                .GET( serverURI.resolve( "/test/tableau/json/" + queryEncoded).toString() );

        System.out.println(response.toString());
        // Then it should reply
        assertEquals(200, response.status());
    }
}