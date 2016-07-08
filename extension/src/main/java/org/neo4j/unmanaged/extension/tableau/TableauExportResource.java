package org.neo4j.unmanaged.extension.tableau;

import com.sun.jersey.core.spi.factory.ResponseBuilderImpl;
import com.tableausoftware.TableauException;
import com.tableausoftware.server.ServerAPI;
import com.tableausoftware.server.ServerConnection;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response.Status;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.helpers.collection.ArrayIterator;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response.ResponseBuilder;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphdb.Transaction;

@Path( "/tableau" )
public class TableauExportResource
{
    private final GraphDatabaseService graphDb;
    
    private final ArrayList<String> columns;
    private final Map<String, Neo4jType> columnMeta;
    private final Map<String, Integer> columnPos;
    private final List<Map<String, Object>> resultset;
    private String colNameDelim = " ";
    private boolean capitalizeNames = true;
 
    // max amount of records to inspect column data types:
    private final int maxRecCntInspect = 1000;
    private final String fileName = System.getProperty("java.io.tmpdir") + "cypherquery.tde";
    private TdeWriter tdeWriter;
    private final ObjectMapper mapper = new ObjectMapper();
    
    public TableauExportResource( @Context GraphDatabaseService database )
    {
        this.graphDb = database;
        this.columns = new ArrayList<>();
        this.columnMeta = new HashMap();
        this.columnPos = new HashMap();
        this.resultset = new ArrayList<>();
    }

    @GET
    @Produces({ "application/x-download", MediaType.APPLICATION_JSON })
    @Path( "/tde/{cypher}" )
    public Response tde( @PathParam( "cypher" ) String cypherQuery )
    {
        String strOutput = "";
        List<String> rows = new ArrayList<>();
        Map<String, Object> rowSet;
        
        // operations on the graph
        this.tdeWriter = new TdeWriter(this.fileName, this.maxRecCntInspect, this.capitalizeNames, this.colNameDelim);

        try
        (  Transaction tx = graphDb.beginTx()) {

//        try ( Result result = graphDb.execute( cypherQuery, parameters ) )
            System.out.println("Execute query " + cypherQuery);

            try ( Result result = graphDb.execute( cypherQuery ) )
            {
                if (result.hasNext() ) {
                    // process query result
                    while ( result.hasNext() )
                    {
                        rowSet = new HashMap();
                        Map<String, Object> row = result.next();
                        for ( String key : result.columns() )
                        {
                            processRowKey( key, row, rowSet );
                        }
                        this.resultset.add( rowSet );               
                        this.tdeWriter.insertData(this.resultset, this.columns, this.columnMeta, this.columnPos);
                    }
                    this.tdeWriter.setResultSetFinished();
                    this.tdeWriter.insertData(this.resultset, this.columns, this.columnMeta, this.columnPos);
                    tx.success();
                } else {
                    this.tdeWriter.addException( (new TableauExportMessage( "Cypher Statement", "Returned no rows.." )).toJson() );
                }
            } catch(Exception ex) {
                this.tdeWriter.addException( (new TableauExportMessage( ex )).toJson() );
                tx.terminate();
            }
        }       

        this.tdeWriter.close();

        if (this.tdeWriter.isExtractCreated() && !this.tdeWriter.isErrorOccured()) {
            // return TDE file
            File file = new File( this.tdeWriter.getFileName() );
            ResponseBuilder response = Response.ok((Object) file);
            response.header("Content-Disposition",
                            "attachment; filename=" + file.getName() );
            return response.build();            
        } else {
            // return error message
            ResponseBuilderImpl builder = new ResponseBuilderImpl();
            builder.status(Status.BAD_REQUEST);
            builder.entity( this.tdeWriter.getExceptionList().toString() );
            builder.type(MediaType.APPLICATION_JSON);
            Response response = builder.build();
            throw new WebApplicationException(response);
        }
    }
    
    @GET
    @Produces({ MediaType.APPLICATION_JSON })
    @Path( "/tdepublish/{project}/{datasource}/{cypher}" )
    public Response tdePublish( 
            @PathParam( "project" ) String project, // "default"
            @PathParam( "datasource" ) String datasource, // "Cypher-Query"
            @PathParam( "cypher" ) String cypherQuery 
        )
    {
        String strOutput = "";
        List<String> rows = new ArrayList<>();
        Map<String, Object> rowSet;
        
        if (project.equals("")) project = "default";
        if (datasource.equals("")) datasource = "Cypher-Query";
        
        this.tdeWriter = new TdeWriter(this.fileName, maxRecCntInspect, capitalizeNames, colNameDelim);

        try
        (  Transaction tx = graphDb.beginTx()) {
            // operations on the graph

//        try ( Result result = graphDb.execute( cypherQuery, parameters ) )
            System.out.println("Execute query " + cypherQuery);

            try ( Result result = graphDb.execute( cypherQuery ) )
            {
                if (result.hasNext() ) {
                    // process query result
                    while ( result.hasNext() )
                    {
                        rowSet = new HashMap();
                        Map<String, Object> row = result.next();
                        for ( String key : result.columns() )
                        {
                            processRowKey( key, row, rowSet );
                        }
                        this.resultset.add( rowSet );               
                        this.tdeWriter.insertData(this.resultset, this.columns, this.columnMeta, this.columnPos);
                    }
                    this.tdeWriter.setResultSetFinished();
                    this.tdeWriter.insertData(this.resultset, this.columns, this.columnMeta, this.columnPos);
                    tx.success();
                } else {
                    this.tdeWriter.addException( (new TableauExportMessage( "Cypher Statement", "Returned no rows.." )).toJson() );
                }
            } catch(Exception ex) {
                tx.terminate();
                this.tdeWriter.addException( (new TableauExportMessage( ex )).toJson() );
            }
        }       

        this.tdeWriter.close();
        
        ResponseBuilderImpl builder = new ResponseBuilderImpl();
        if (this.tdeWriter.isExtractCreated() && !this.tdeWriter.isErrorOccured()) {
            try {
                // Raed properties
                Properties prop = new Properties();
//                System.out.println("Working Directory = " + System.getProperty("user.dir"));
                String workingdir = System.getProperty("user.dir");
                // fix for Windows environment
                if(workingdir.substring(workingdir.length()-4).equals("\\bin")) workingdir = workingdir.substring(0, workingdir.length()-4);
                prop.load(new FileInputStream(workingdir + "/plugins/tableau-server.properties"));
                
                String serverUrl, userName, passWord, siteId;
                serverUrl = prop.getProperty("server.url");
                userName = prop.getProperty("username");
                passWord = prop.getProperty("password");
                siteId = prop.getProperty("site.id");
                
                // publsih TDE file
                // Initialize Tableau Server API
                ServerAPI.initialize();
                // Create the server connection object
                ServerConnection serverConnection = new ServerConnection();

                // Connect to the server
                System.out.println("Connect to " + serverUrl);
                serverConnection.connect(serverUrl, userName, passWord, siteId);

                // Publish tde to the server under the default project with name Cypher-Query
                System.out.println("Publish file " + this.fileName);
                serverConnection.publishExtract(this.fileName, project, datasource, true);

                // Disconnect from the server
                serverConnection.disconnect();

                // Destroy the server connection object
                serverConnection.close();

                // Clean up Tableau Server API
                ServerAPI.cleanup();

                builder.status(Status.OK);
                builder.entity( (new TableauExportMessage( "TDE Server Publishing", "TDE file " + this.fileName 
                        + " published to server:" + serverUrl )).toJson() );
                builder.type(MediaType.APPLICATION_JSON);
                return builder.build();
            } catch (TableauException | IOException ex) {
                Logger.getLogger(TableauExportResource.class.getName()).log(Level.SEVERE, null, ex);
                this.tdeWriter.addException( (new TableauExportMessage( ex )).toJson() );
            }
        }
        // return error message
        builder.status(Status.BAD_REQUEST);
        builder.entity( this.tdeWriter.getExceptionList().toString() );
        builder.type(MediaType.APPLICATION_JSON);
        Response response = builder.build();
        throw new WebApplicationException(response);                 
    }
    
    @GET
    @Produces( MediaType.TEXT_PLAIN )
    @Path( "/json/{cypher}" )
    // for testing purpose only
    public Response json( @PathParam( "cypher" ) String cypherQuery )
    {
        List<String> rows = new ArrayList<>();
        Map<String, Object> rowSet;
        this.colNameDelim = "_";
        this.capitalizeNames = false;
        
        try
        ( Transaction tx = graphDb.beginTx()) {
            // operations on the graph

//        try ( Result result = graphDb.execute( cypherQuery, parameters ) )
            try ( Result result = graphDb.execute( cypherQuery ) )
            {
                while ( result.hasNext() )
                {
                    rowSet = new HashMap();
                    Map<String, Object> row = result.next();
                    for ( String key : result.columns() )
                    {
                        processRowKey( key, row, rowSet );
                    }
                    this.resultset.add( rowSet );
                }
            }
            tx.success();
        }       
  
        // build array of JSON objects/row
        Iterator iter = resultset.iterator();
        while (iter.hasNext()) {
            try {
                rows.add(this.mapper.writeValueAsString(iter.next()));
            } catch (IOException ex) {
                Logger.getLogger(TableauExportResource.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        
        // return JSON string
        return Response.status( Status.OK ).entity(
                rows.toString().getBytes( Charset.forName("UTF-8") ) ).build();
    }

    private void processRowKey(String key, Map<String, Object> row, Map<String, Object> rowSet) {
        Object value = row.get( key );
        if (value instanceof Node) {
            processNode( key, (Node)value, rowSet );                                       
        } else
        if (value instanceof Relationship) {
            processRelationship( key, (Relationship)value, rowSet );
        } else
//        if (value instanceof Iterable) {
//            for (Object inner : (Iterable) value) {
//                addToGraph(graph,inner);
//            }
//        } else                    
        {
            // Property (eg. n.name)
            processProperty(key, "", value, rowSet );
        }
    }
    
    private void processNode(String col, Node node, Map<String, Object> rowSet) {        
        addColumn( col + this.colNameDelim + "id", Neo4jType.LONG );
        rowSet.put( col + this.colNameDelim + "id", new Long(node.getId()) );
        
        try {
            for ( Label label : node.getLabels() )
            {
                addColumn( col + this.colNameDelim + "label", Neo4jType.STRING);
                rowSet.put( col + this.colNameDelim + "label", label.name() );
            }
        } catch (Exception e) {
            // Node has no labels
        }

        try {
            for (String key : node.getPropertyKeys())
            {              
                processProperty( col, key, node.getProperty( key ), rowSet );
            }
        } catch (Exception e) {
            // Node has no properties
        }
    }
    
    private void processRelationship(String col, Relationship relation, Map<String, Object> rowSet) {
        addColumn( col + this.colNameDelim + "id", Neo4jType.LONG );
        rowSet.put( col + this.colNameDelim + "id", new Long(relation.getId()) );
        
        try {
            addColumn(col + this.colNameDelim + "type", Neo4jType.STRING);
            rowSet.put( col + this.colNameDelim + "type", relation.getType().name() );
        } catch (Exception e) {
            // Relation has no type
        }
                
        try {
            for (String key : relation.getPropertyKeys())
            {              
                processProperty( col, key, relation.getProperty( key ), rowSet );
            }
        } catch (Exception e) {
            // Relation has no properties
        }
    }
    
    private void processProperty(String col, String sub, Object value, Map<String, Object> rowSet) {
        int cnt = 0;
        if (!sub.equals("")) {
            sub = this.colNameDelim + sub;
        }
        
        if (value instanceof String) {
            addColumn( col + sub, Neo4jType.STRING );
            rowSet.put( col + sub, (String)value );
        } else
        if (value instanceof Long) {
            if (((Long)value) > Integer.MAX_VALUE) {
                addColumn( col + sub, Neo4jType.DOUBLE );
            } else {
                addColumn( col + sub, Neo4jType.LONG );
            }
            rowSet.put( col + sub, ((Long)value) );
        } else
        if (value instanceof Double) {
            addColumn(col + sub, Neo4jType.DOUBLE);
            rowSet.put( col + sub, ((Double)value) );
        } else
        if (value instanceof Boolean) {
            addColumn(col + sub, Neo4jType.BOOLEAN);
            rowSet.put( col + sub, ((Boolean)value) );
        } else
        if (value instanceof Object[]) {
            ArrayIterator iter = new ArrayIterator( (Object[])value );
            while ( iter.hasNext() )
            {
                processProperty(col + sub, "" + cnt, iter.next(), rowSet);
                cnt ++;               
            }
        } else {
            addColumn(col + sub, Neo4jType.UNKNOWN);
        }
    }
    
    private int addColumn(String columnName, Neo4jType type) {
        int pos = -1;
        
        if (this.tdeWriter.getRecCnt() <= this.tdeWriter.getMaxRecCntInspect()) {
            Neo4jType actualType = this.columnMeta.get(columnName);
            if (actualType == null) {
                // column doesn't exist
                this.columnMeta.put(columnName, type);
                this.columns.add(columnName);
                pos = this.columns.size() -1;
                columnPos.put(columnName, new Integer(pos));
            } else if (actualType != type) {
                // new column type STRING inspected, upgrade type if possible
                if (actualType == Neo4jType.LONG && (type == Neo4jType.DOUBLE || type == Neo4jType.STRING)) {
                    this.columnMeta.put(columnName, type);
                } else if ((actualType == Neo4jType.DOUBLE || actualType == Neo4jType.BOOLEAN) && type == Neo4jType.STRING) {
                    this.columnMeta.put(columnName, type);
                }
            }
        }
        return pos;     
    }
}