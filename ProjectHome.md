This framework is intended to determine the ability of various graph databases to load a large volume of linked data.
The test data is all of English Wikipedia's article link data e.g. 130 million records of the form:   Communism->Russia

Example databases currently supported include Neo4j (via it's batch loading API) and OrientDB.
The test code also includes alternative index services that can maintain a growing index to look up a key (e.g. "Russia") and find any previously inserted Vertex id. These are based on Lucene and Berkeley DB implementations. A new key-value store is also included here that shows a lot of promise and is outlined here: http://www.slideshare.net/MarkHarwood/lucene-kvstore

NB: the Neo4J implementations of the loader service included here will be GPL and not Apache License due to the Neo4J licensing model.