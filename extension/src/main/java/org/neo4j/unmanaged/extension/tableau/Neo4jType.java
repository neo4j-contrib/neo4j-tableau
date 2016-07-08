/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.neo4j.unmanaged.extension.tableau;

import java.util.Arrays;

/**
 *
 * @author ralfbecher
 */
public enum Neo4jType {
    BOOLEAN,
//    BYTE,
//    SHORT,
//    INT,
    LONG,
//    FLOAT,
    DOUBLE,
//    CHAR,
    STRING,
    UNKNOWN;

    private static final String ALL_TYPES_STRING = Arrays.toString(Neo4jType.values());

    public static Neo4jType getType(Class<?> clazz) {
        String className = clazz.getSimpleName().toUpperCase();
        if (ALL_TYPES_STRING.contains(className)) {
            return Neo4jType.valueOf(className);
        } else {
            return UNKNOWN;
        }
    }    
}
