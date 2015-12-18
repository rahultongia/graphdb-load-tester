# GraphDB Results #
| **Technology** | **Description** | **Results**|
|:---------------|:----------------|:-----------|
| **OrientDB**   | The Orient DB database implementation suffered from very slow load times despite using the transactional API to batch operations. | The KVStore service can improve OrientDB performance but is still slow |
| **Neo4J**      |The Neo4J implementation uses the batch inserter API and the choice of higher performance indexing service discussed below to provide the Wikipedia-article-ID to Vertex-id lookup.|After 30 million additions the Neo4J database performance [hit a wall](https://spreadsheets.google.com/ccc?key=0AsKVSn5SGg_wdE5KWXFwd0JuRGlfR2NOMG5KVUVOWlE&hl=en&authkey=CLnptdAN) and the load process had to be killed. [Subsequent conversations](http://lists.neo4j.org/pipermail/user/2011-February/006969.html) have revealed that the memory required by Neo4J is directly proportional to the number of edges and at least 4 gig of RAM will be required to complete this particular test. I will tweak the config settings and repeat this test when I have access to appropriate hardware.|
| **InfiniteGraph** |                 | This database will not be partaking in these tests at this time |
| **Lucene Graph** | Not strictly a graph database - using a Lucene document for each node and each edge with indexed properties for Article IDs | All 130 million records loaded into an 11GB index in 6 hours using < 1.5GB RAM with reasonably flat performance |


## Indexing Services ##
A null graph is available for testing the performance of indexing services without a full graph storing edges and nodes. The results show that the KVStore implementation is the fastest - see http://goo.gl/VQ027

![http://www.inperspective.com/lucene/chart_1.png](http://www.inperspective.com/lucene/chart_1.png)