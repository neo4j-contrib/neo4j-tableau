package org.neo4j.unmanaged.extension.tableau;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.codehaus.jackson.map.ObjectMapper;

/**
 *
 * @author ralfbecher
 */
public class TableauExportMessage {

    private String cause;
    private String message;
    private final ObjectMapper mapper = new ObjectMapper();

    public String getCause() {
        return cause;
    }

    public String getMessage() {
        return message;
    }

    public TableauExportMessage(Exception ex) {
        String cs = "unkown";
        if (ex.getCause() != null) {
            cs = ex.getCause().toString();       
        }
        if (cs != null && cs.indexOf(":") > 0) {
            cs = cs.substring(0, cs.indexOf(":"));
        }
        this.cause = cs;
        this.message = ex.getMessage();
    }

    public TableauExportMessage(String cs, String msg) {
        this.cause = cs;
        this.message = msg;
    }
    
    public String toJson() {
        try {
            return this.mapper.writeValueAsString( this );
        } catch (IOException ex1) {
            Logger.getLogger(TableauExportResource.class.getName()).log(Level.SEVERE, null, ex1);
            return null;
        }
    }

}
