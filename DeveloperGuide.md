# Preparing the test environment #
This package does **not** include the Wikipedia link dataset which is ~1GB of zipped content.
The ant build.xml file should automatically download the appropriate data file before running the tests.

# Configuring different test scenarios #
The Spring configuration file contains different choices for configurations of graph loading services.
The choices of graph database are currently:
  * Neo4j 1.6.1
  * OrientDB 1.0rc8
  * A mock graph used to test only the choice of indexing service

Each graph database also needs an indexing service. It should be noted that the "KVStore" class provided here offers a faster indexing service that is capable of dealing with the sorts of data volumes in this test.


At the time of writing none of these configurations are capable of loading the test data on Windows 32 bit JVM or on Mac OSX with 2GB RAM.