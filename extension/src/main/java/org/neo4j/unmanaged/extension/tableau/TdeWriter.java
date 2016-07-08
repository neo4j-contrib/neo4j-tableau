/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.neo4j.unmanaged.extension.tableau;

import com.tableausoftware.TableauException;
import com.tableausoftware.common.Collation;
import com.tableausoftware.common.Type;
import com.tableausoftware.extract.Extract;
import com.tableausoftware.extract.ExtractAPI;
import com.tableausoftware.extract.Row;
import com.tableausoftware.extract.Table;
import com.tableausoftware.extract.TableDefinition;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang3.text.WordUtils;

/**
 *
 * @author ralfbecher
 */
public class TdeWriter {
    // Tableau API
    private Extract extract;
    private TableDefinition tableDef;
    private Table table;
    
    private final int maxRecCntInspect;
    private int recCnt = 0;
    
    private boolean extractOpen = false;
    private boolean extractCreated = false;
    private boolean resultSetFinished = false;
    private String fileName;
    private boolean capitalizeNames = true;
    private String colNameDelim = " ";
    private final List<String> exceptionList;

    public TdeWriter(String fileName, int maxRecCntInspect, boolean capitalizeNames, String colNameDelim) {
        this.fileName = fileName;
        this.maxRecCntInspect = maxRecCntInspect; 
        this.capitalizeNames = capitalizeNames;
        this.colNameDelim = colNameDelim;
        this.exceptionList = new ArrayList<>();
       
        try {
            // Initialize Tableau Extract API
            ExtractAPI.initialize();
        } catch (TableauException ex) {
            // however, this gets an exception on Windows, do not log this:
            // com.tableausoftware.TableauException: Unknown error
            //Logger.getLogger(TableauExportResource.class.getName()).log(Level.SEVERE, null, ex);
            //this.exceptionList.add( (new TableauExportMessage( ex )).toJson() );
        }                

    }

    public boolean isExtractOpen() {
        return extractOpen;
    }

    public boolean isExtractCreated() {
        return extractCreated;
    }

    public boolean isResultSetFinished() {
        return resultSetFinished;
    }

    public boolean isErrorOccured() {
        return (this.exceptionList.size() > 0);
    }

    public String getFileName() {
        return fileName;
    }

    public void setResultSetFinished() {
        this.resultSetFinished = true;
    }

    public List<String> getExceptionList() {
        return exceptionList;
    }

    public void addException(String message) {
        this.exceptionList.add( message );
    }

    public int getRecCnt() {
        return recCnt;
    }

    public int getMaxRecCntInspect() {
        return maxRecCntInspect;
    }
        
    public void insertData(List<Map<String, Object>> resultset, 
            ArrayList<String> columns, Map<String, Neo4jType> columnMeta, 
            Map<String, Integer> columnPos) {
        Row extractRow;
        Map<String, Object> rowSet;

        this.recCnt ++;
        // create
        if (this.recCnt >= this.maxRecCntInspect || this.resultSetFinished) {
            createExtract(columns, columnMeta);
        }
        // insert data
        if (this.extractOpen) {
            Iterator iterRow = resultset.iterator();
            // iterate rows
            while (iterRow.hasNext()) {
                try {
                    // create new extract row
                    extractRow = new Row(this.tableDef);
                    rowSet = ((HashMap)iterRow.next());
                    for (String column : columns) {
                        Object value = rowSet.get(column);
                        if (value == null) {   
                            extractRow.setNull(getColumnPos( column,columnPos ));
                        } else {
                            switch ( getColumnType( column, columnMeta ) ) {
                                case BOOLEAN:
                                    extractRow.setBoolean(getColumnPos( column, columnPos), ((Boolean)value).booleanValue()); 
                                    break;
                                case LONG:
                                    extractRow.setInteger(getColumnPos( column, columnPos ), ((Long)value).intValue());                            
                                    break;
                                case DOUBLE:
                                    if (value instanceof Long) {
                                        extractRow.setDouble(getColumnPos( column, columnPos ), ((Long)value).doubleValue());                            
                                    } else {
                                        extractRow.setDouble(getColumnPos( column, columnPos ), ((Double)value).doubleValue());  
                                    }
                                    break;
                                default:
                                    // String or upgraded to String
                                    if (value instanceof String) {
                                        extractRow.setString(getColumnPos( column, columnPos ), (String)value);                            
                                    } else
                                    if (value instanceof Long) {
                                        extractRow.setString(getColumnPos( column, columnPos), ((Long)value).toString());
                                    } else
                                    if (value instanceof Double) {
                                        extractRow.setString(getColumnPos( column, columnPos), ((Double)value).toString());  
                                    } else 
                                    if (value instanceof Boolean) {
                                        extractRow.setString(getColumnPos( column, columnPos), ((Boolean)value).toString());                            
                                    } else {
                                        extractRow.setString(getColumnPos( column, columnPos ), (String)value);                            
                                    }
                                    break;
                            }
                        }         
                    }
                    // insert row into extract
                    this.table.insert(extractRow);
                    // delete row from resultset
                    iterRow.remove();
                } catch (TableauException ex) {
                    Logger.getLogger(TableauExportResource.class.getName()).log(Level.SEVERE, null, ex);
                    this.exceptionList.add( (new TableauExportMessage( ex )).toJson() );
                }
            }
        }
    }
  
    // Define the TDE table's schema and create extract file
    private void createExtract(ArrayList<String> columns, Map<String, Neo4jType> columnMeta) {
        if (!this.extractOpen) {
            try {
                this.tableDef = new TableDefinition();
                this.tableDef.setDefaultCollation(Collation.EN_GB);
                for (String column : columns) {
                    Neo4jType value = columnMeta.get(column);
                    this.tableDef.addColumn( capitalizeColumnName(column) , getTableauType( value ) );
                }
                printTableDefinition();
                
                deleteFile(this.fileName);
                System.out.println("Create file "+ fileName);
          
                this.extract = new Extract(this.fileName);
                this.table = this.extract.addTable("Extract", this.tableDef);                
                this.extractOpen = true;
                
            } catch (TableauException ex) {
                Logger.getLogger(TableauExportResource.class.getName()).log(Level.SEVERE, null, ex);
                this.exceptionList.add( (new TableauExportMessage( ex )).toJson() );
            }
        }
    }
    
    private String capitalizeColumnName(String column) {
        if (this.capitalizeNames) {
            return WordUtils.capitalizeFully(column, colNameDelim.toCharArray());
        } else {
            return column;
        }
    }
    
    private Neo4jType getColumnType(String columnName, Map<String, Neo4jType> columnMeta) {
        return columnMeta.get(columnName);
    }
    
    private int getColumnPos(String columnName, Map<String, Integer> columnPos) {
        Integer pos = columnPos.get(columnName);
        if (pos == null) {
            return -1;
        } else {
            return pos.intValue();
        } 
    }

    private Type getTableauType(Neo4jType type) {
        // Neo4Type Mapping for TDE Types: 
        // Tableau: INTEGER, DOUBLE, BOOLEAN, DATE, DATETIME, DURATION, CHAR_STRING, UNICODE_STRING
        switch (type) {
            case BOOLEAN:
                return Type.BOOLEAN;
            case LONG:
                return Type.INTEGER;
        case DOUBLE:
                return Type.DOUBLE;
            default:    
                return Type.UNICODE_STRING;
        }
    }

    // Print a Table's schema to stdout.
    private void printTableDefinition() throws TableauException {
        int numColumns = this.tableDef.getColumnCount();
        for ( int i = 0; i < numColumns; ++i ) {
            Type type = this.tableDef.getColumnType(i);
            String name = this.tableDef.getColumnName(i);
            System.out.format("Column %d: %s %s\n", i, name, type.toString());
        }
    }
    
    private static boolean deleteFile(String fileName) {
        File f = new File(fileName);
        if (f.exists()) {
            System.out.println("Delete file "+ fileName);
            if (!f.delete()) {
                Logger.getLogger(TableauExportResource.class.getName()).log(Level.SEVERE, "Deletion of file {0} failed!", fileName);
                return false;
            }
        }
        return true;
    }

    public void close() {
        if (this.extractOpen) {
            System.out.println("Close tde file");
            // Clean up Tableau Extract API
            this.extract.close();
        }
        try {
            ExtractAPI.cleanup();
        } catch (TableauException ex) {
            //Logger.getLogger(TdeWriter.class.getName()).log(Level.SEVERE, null, ex);
            //this.exceptionList.add( (new TableauExportMessage( ex )).toJson() );
        }
        this.extractOpen = false; 
        this.extractCreated = true;  
    }
}
