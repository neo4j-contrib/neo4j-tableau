# install dependencies
mvn install:install-file -Dpackaging=jar -DgroupId=com.tableausoftware -Dversion=9.1.0 -DartifactId=tableau-extract -Dfile=jars/tableau-extract-9.1.0.jar -DgeneratePom=true
mvn install:install-file -Dpackaging=jar -DgroupId=com.tableausoftware -Dversion=9.1.0 -DartifactId=tableau-common  -Dfile=jars/tableau-common-9.1.0.jar  -DgeneratePom=true
mvn install:install-file -Dpackaging=jar -DgroupId=com.tableausoftware -Dversion=9.1.0 -DartifactId=tableau-server  -Dfile=jars/tableau-server-9.1.0.jar  -DgeneratePom=true
mvn install:install-file -Dpackaging=jar -DgroupId=com.sun.jna -DartifactId=jna -Dversion=3.5.1 -Dfile=jars/jna-3.5.1.jar  -DgeneratePom=true

# build extension
mvn clean install

# create zip
zip -rj neo4j-tableau-3.0.zip jars/*.jar target/neo4j-unmanaged-extension-tableau-*.jar