# TDE File Generator

The code in this package is an unmanaged extension for Neo4j server which generates
TDE (Tableau Data Extract) files.

These files can be used to integrate data resulting from cypher queries in neo4j, into
tableau visualizations.

For more information, see the [blog post](https://neo4j.com/blog/neo4j-tableau-integration/)
on Neo4J and Tableau integration.

## Build

Run the `./build.sh` script; you'll need to have maven installed.

## Install & Configure

Copy all of the JAR files from the `jars/` subdirectory into the `plugins` directory of your
server.

Add the following line to your neo4j.conf:

```dbms.unmanaged_extension_classes=org.neo4j.unmanaged.extension.tableau=/export```

This line maps invocation of the classes provided in this package to the `/export` URL on
the server.  In turn the package maps a number of URLs such as:

* `/export/tableau/tde/{cypher}`
* `/export/tde/{cypher}`
* `/export/tdepublish/{project}/{datasource}/{cypher}`
* `/export/tdepublish/{project}/{datasource}`

