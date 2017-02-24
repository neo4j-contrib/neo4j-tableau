var config = {};

function executeNeoQueries(config, username, password, mode, fnResults, fnReason) {
    return new Promise(function (resolve, reject) {
        if ((config.url) && (username) && (password)) {
            var driver = neo4j.v1.driver(config.url, neo4j.v1.auth.basic(username, password));
            var session = driver.session();
            var p = [];
            var resultsTable = [];
            config.cypherQueries.forEach(function (query, i) {
                if (!(query.trim() === "")) {
                    if (mode === 0) {
                        // Explain Query
                        query = 'Explain ' + query;
                    }
                    if (mode === 1) {
                        // inspect schema query
                        var inspectRows = parseInt(config.inspectRows);
                        var queryWords = query.split(/[\s\n]+/);
                        if (isNaN(inspectRows) || inspectRows == 0) inspectRows = 1000;
                        if (queryWords.length > 2 && queryWords[queryWords.length - 2].toLowerCase() === "limit") {
                            var limit = Number(queryWords[queryWords.length - 1]);
                            if (!isNaN(limit) && limit > inspectRows) {
                                queryWords[queryWords.length - 1] = inspectRows.toString();
                                query = queryWords.join(" ");
                            }
                        } else {
                            query = query + " limit " + inspectRows;
                        }
                    }
                    resultsTable.push(config.tableNames[i]);
                    p.push(session.run(query));
                }
            });
            if (p.length > 0) {
                Promise.all(p).then(function (results) {
                    session.close();
                    var resultsData = fnResults(mode, results);
                    resolve({
                        resultsTable: resultsTable,
                        resultsData: resultsData
                    });
                }, function (reason) {
                    session.close();
                    fnReason(mode, reason);
                    reject();
                });
            } else {
                session.close();
                resolve();
            }
        }
    });
}

var fnQueryResults = function (mode, results) {
    results = results || {};
    var data = [],
        rows = 0;

    // Helper functions
    var recordToNative = function (rec) {
        var out = {}
        rec.keys.forEach(function (key, index) {
            out[key] = rec._fields[index]
        })
        return out
    }
    var isRecord = function (obj) {
        if (typeof obj._fields !== 'undefined' && typeof obj.keys !== 'undefined') {
            return true
        }
        return false
    }
    var mapObj = function (fn, obj) {
        var out = {}
        Object.keys(obj).forEach(function (key) {
            if(key.indexOf('.') === -1) {
                out[key] = fn(obj[key])
            } else {
                out[key.replace(/\./g, '_')] = fn(obj[key])            
            }
        })
        return out
    }

    // Main function to be exposed / used
    var toNative = function (val) {
        if (val === null) return val
        if (val instanceof neo4j.v1.types.Node) return toNative(val.properties)
        if (val instanceof neo4j.v1.types.Relationship) return toNative(val.properties)
        if (neo4j.v1.isInt(val)) return val.toInt()
        if (Array.isArray(val)) return val.map(function (a) { return toNative(a) })
        if (isRecord(val)) return toNative(recordToNative(val))
        if (typeof val === 'object') return mapObj(toNative, val)
        return val
    }

    if (results.length > 0) {
        //loop results
        results.forEach(function (result, i) {
            rows = 0;
            recordObjects = [];
            if (mode === 0) {
                if (result.summary.plan) {
                    // Explain query
                    rows = result.summary.plan.arguments.EstimatedRows;
                }
                if (rows === 0) {
                    sweetAlert("Cypher Query " + (i + 1), "Result Set for query " + (i + 1) + " is empty!", "warning");
                    return [];
                }
            } else {
                // Cypher query
                recordObjects = result.records.map(function (e) {
                    return toNative(e);
                });
                data.push(recordObjects);
            }
        });
        if (mode === 0 && rows > 0) {
            tableau.connectionData = JSON.stringify({
                "config": config
            });
            tableau.submit();
        }
        return data;
    } else {
        if (mode === 0) {
            sweetAlert("Cypher Query", "No query result!", "warning");
            return [];
        } else {
            tableau.abortWithError({
                error: "No query result!"
            });
        }
    }
};

var fnQueryReason = function (mode, reason) {
    reason = reason || {};
    if (reason.hasOwnProperty('message')) {
        if (mode === 0) {
            sweetAlert("Cypher Error", reason.message, "error");
        } else {
            tableau.abortWithError({
                error: "Cypher Error: " + reason.message
            });
        }
    } else if (reason.fields.length > 0) {
        if (mode === 0) {
            sweetAlert("Cypher Error", reason.fields[0].code + "\n" + reason.fields[0].message, "error");
        } else {
            tableau.abortWithError({
                error: "Cypher Error: " + reason.fields[0].code + "\n" + reason.fields[0].message
            });
        }
    }
};

(function () {
    var myConnector = tableau.makeConnector();

    myConnector.getSchema = function (schemaCallback) {
        if (!(tableau.username) || !(tableau.password)) {
            tableau.abortWithError({
                error: "Please enter connection credentials again via Edit Data Source: username/password\n(Doesn't get stored into the Tableau Workbook file.)"
            });
        }

        var connData = JSON.parse(tableau.connectionData);

        executeNeoQueries(connData.config, tableau.username, tableau.password, 1, fnQueryResults, fnQueryReason).then(function (results) {
            var tables = [];
            results.resultsData.forEach(function (data, i) {
                var flatData = _jsToTable(data);
                var columns = [];
                for (var key in flatData.headers) {
                    columns.push({
                        "id": key,
                        "alias": toTitleCase(key),
                        "dataType": flatData.headers[key]
                    });
                }
                var table = {
                    "id": results.resultsTable[i],
                    "alias": results.resultsTable[i],
                    "columns": columns
                };
                tables.push(table);
            });
            // cache meatadata and data for use in getData()
            myConnector.tables = tables;
            schemaCallback(tables);
        });
    };

    myConnector.getData = function (table, doneCallback) {
        var pos = -1;
        myConnector.tables.forEach(function (e, i) {
            if (e.id === table.tableInfo.id) {
                pos = i;
            }
        });

        var flatData = [], newRowData = [];
        var connData = JSON.parse(tableau.connectionData);
        connData.config.cypherQueries = [connData.config.cypherQueries[pos]];
        connData.config.tableNames = [myConnector.tables[pos].id];

        executeNeoQueries(connData.config, tableau.username, tableau.password, 2, fnQueryResults, fnQueryReason).then(function (results) {
            flatData = _jsToTable(results.resultsData[0]);
            newRowData = flatData.rowData.map(function (record) {
                for (var key in flatData.headers) {
                    if (!record.hasOwnProperty(key)) {
                        record[key] = null;
                    }
                }
                return record;
            });
            flatData = null;

            if (newRowData.length === 0) {
                tableau.abortWithError({
                    error: "No Data for Table " + pos
                });
            }

            table.appendRows(newRowData);
            doneCallback();
        });
    };

    function toTitleCase(str) {
        return str.split(/[_.]+/)
            .map(function (i) {
                return i[0].toUpperCase() + i.substring(1);
            })
            .join(' ');
    }

    tableau.registerConnector(myConnector);
})();

// Takes a hierarchical javascript object and tries to turn it into a table
// Returns an object with headers and the row level data
function _jsToTable(objectBlob) {
    var rowData = _flattenData(objectBlob);
    var headers = _extractHeaders(rowData);
    return {
        "headers": headers,
        "rowData": rowData
    };
}

// Given an object:
//   - finds the longest array in the object
//   - flattens each element in that array so it is a single object with many properties
// If there is no array that is a descendent of the original object, this wraps
// the input in a single element array.
function _flattenData(objectBlob) {
    // first find the longest array
    var longestArray = _findLongestArray(objectBlob, []);
    if (!longestArray || longestArray.length == 0) {
        // if no array found, just wrap the entire object blob in an array
        longestArray = [objectBlob];
    }
    for (var ii = 0; ii < longestArray.length; ++ii) {
        _flattenObject(longestArray[ii]);
    }
    return longestArray;
}

// Given an object with hierarchical properties, flattens it so all the properties
// sit on the base object.
function _flattenObject(obj) {
    for (var key in obj) {
        if (obj.hasOwnProperty(key) && typeof obj[key] == 'object') {
            var subObj = obj[key];
            _flattenObject(subObj);
            for (var k in subObj) {
                if (subObj.hasOwnProperty(k)) {
                    obj[key + '_' + k] = subObj[k];
                }
            }
            delete obj[key];
        }
    }
}

// Finds the longest array that is a descendent of the given object
function _findLongestArray(obj, bestSoFar) {
    if (!obj) {
        // skip null/undefined objects
        return bestSoFar;
    }

    // if an array, just return the longer one
    if (obj.constructor === Array) {
        // I think I can simplify this line to
        // return obj;
        // and trust that the caller will deal with taking the longer array
        return (obj.length > bestSoFar.length) ? obj : bestSoFar;
    }
    if (typeof obj != "object") {
        return bestSoFar;
    }
    for (var key in obj) {
        if (obj.hasOwnProperty(key)) {
            var subBest = _findLongestArray(obj[key], bestSoFar);
            if (subBest.length > bestSoFar.length) {
                bestSoFar = subBest;
            }
        }
    }
    return bestSoFar;
}

// Given an array of js objects, returns a map from data column name to data type
function _extractHeaders(rowData) {
    var toRet = {};
    for (var row = 0; row < rowData.length; ++row) {
        var rowLine = rowData[row];
        for (var key in rowLine) {
            if (rowLine.hasOwnProperty(key)) {
                if (!(key in toRet)) {
                    toRet[key] = _determineType(rowLine[key]);
                }
            }
        }
    }
    return toRet;
}

// Given a primitive, tries to make a guess at the data type of the input
function _determineType(primitive) {
    // possible types: 'float', 'date', 'datetime', 'bool', 'string', 'int'
    if (parseInt(primitive) == primitive) return 'int';
    if (parseFloat(primitive) == primitive) return 'float';
    if (isFinite(new Date(primitive).getTime())) return 'datetime';
    return 'string';
}

function trimQuery(query) {
    if (query && query.length > 0) {
        query = query.trim();
        if (query[query.length - 1] === ';') query = query.slice(0, query.length - 1);
    }
    return query;
}

$(document).ready(function () {
    $('.neo4j-wdc-form .expand').focus(function () {
        var pos = $(this).position();
        $(this).css({
            position: 'absolute',
            top: pos.top,
            left: pos.left,
            "z-index": 10
        });
        $(this).animate({
            height: "12em"
        }, 100);
    });
    $('.neo4j-wdc-form .expand').blur(function () {
        $(this).css({
            height: "25px",
            width: "400px"
        });
        $(this).css({
            position: 'relative',
            top: 0,
            left: 0,
            "z-index": 1
        });
    });

    $('#submitButton').click(function () {
        tableau.connectionName = $('input[name=dataSource]')[0].value.trim();
        config = {
            "url": $('input[name=neo4jUrl]')[0].value.trim(),
            "inspectRows": $('input[name=inspectRows]')[0].value.trim(),
            "tableNames": [
                $('input[name=tableName1]')[0].value.trim(),
                $('input[name=tableName2]')[0].value.trim(),
                $('input[name=tableName3]')[0].value.trim(),
                $('input[name=tableName4]')[0].value.trim(),
                $('input[name=tableName5]')[0].value.trim()
            ],
            "cypherQueries": [
                trimQuery($('textarea[name=cypherQuery1]')[0].value),
                trimQuery($('textarea[name=cypherQuery2]')[0].value),
                trimQuery($('textarea[name=cypherQuery3]')[0].value),
                trimQuery($('textarea[name=cypherQuery4]')[0].value),
                trimQuery($('textarea[name=cypherQuery5]')[0].value)
            ]
        };

        tableau.username = $('input[name=username]')[0].value.trim();
        tableau.password = $('input[name=password]')[0].value.trim();

        if (config.url === "") {
            sweetAlert("Web Data Connector", "Please enter a Neo4j URL to connect...", "info");
        } else if (tableau.username === "" || tableau.password === "") {
            sweetAlert("Web Data Connector", "Please enter credentials to connect Neo4j...", "info");
        } else if (config.cypherQueries.join("").trim() === "") {
            sweetAlert("Web Data Connector", "Please enter a Cypher Query...", "info");
        } else {
            executeNeoQueries(config, tableau.username, tableau.password, 0, fnQueryResults, fnQueryReason);
        }
    });

    if (tableau.connectionName) {
        $('input[name=dataSource]')[0].value = tableau.connectionName;
    }
    if (tableau.connectionData) {
        var connData = JSON.parse(tableau.connectionData);
        $('input[name=neo4jUrl]')[0].value = connData.config.url;
        $('input[name=inspectRows]')[0].value = connData.config.inspectRows;
        $('input[name=tableName1]')[0].value = connData.config.tableNames[0];
        $('input[name=tableName2]')[0].value = connData.config.tableNames[1];
        $('input[name=tableName3]')[0].value = connData.config.tableNames[2];
        $('input[name=tableName4]')[0].value = connData.config.tableNames[3];
        $('input[name=tableName5]')[0].value = connData.config.tableNames[4];
        $('textarea[name=cypherQuery1]')[0].value = connData.config.cypherQueries[0];
        $('textarea[name=cypherQuery2]')[0].value = connData.config.cypherQueries[1];
        $('textarea[name=cypherQuery3]')[0].value = connData.config.cypherQueries[2];
        $('textarea[name=cypherQuery4]')[0].value = connData.config.cypherQueries[3];
        $('textarea[name=cypherQuery5]')[0].value = connData.config.cypherQueries[4];
    }
    if (tableau.username) {
        $('input[name=username]')[0].value = tableau.username;
    }
    if (tableau.password) {
        $('input[name=password]')[0].value = tableau.password;
    }

    $('input[name=dataSource]').focus();
});