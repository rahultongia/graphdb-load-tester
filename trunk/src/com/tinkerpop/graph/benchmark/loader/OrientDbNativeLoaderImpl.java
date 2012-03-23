package com.tinkerpop.graph.benchmark.loader;

import java.io.File;
import java.io.IOException;

import org.apache.lucene.kvstore.KVStore;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.SimpleFSDirectory;

import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OSimpleKeyIndexDefinition;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * A {@link GraphLoaderService} that uses OrientDB via Native APIs
 * @author MAHarwood with help from Luca
 */
public class OrientDbNativeLoaderImpl implements GraphLoaderService
{
    private String orientDbDirName;
    private String orientDbName;
    private OGraphDatabase graph;
    OrientIndex index;
    boolean useInternalOrientIndex = false;

    public OrientDbNativeLoaderImpl()
    {
    }

    public void init()
    {
        File dir = new File(getOrientDbDirName());
        deleteDirectory(dir);
        dir.mkdirs();
        graph = new OGraphDatabase("local:" + getOrientDbDirName());
        if (graph.exists())
        {
            graph.delete();
        }
        graph.create();
        graph.declareIntent(new OIntentMassiveInsert());
        if (useInternalOrientIndex)
        {
            index = new OrientInternalIndex(graph);
        }
        else
        {
            File indexDir = new File("indexes/OrientKVStoreTODO");//TODO parameterize this name
            deleteDirectory(indexDir);
            indexDir.mkdirs();
            Directory luceneDir;
            try
            {
                luceneDir = new SimpleFSDirectory(indexDir);
                KVStore kvStore = new KVStore(luceneDir);
                index = new OrientIndexOnKVStore(kvStore, graph);
            }
            catch (IOException e)
            {
                throw new RuntimeException("Error opening index");
            }
        }
    }

    @Override
    public void addLink(String fromNodeKey, String toNodeKey)
    {
        ODocument fromNode = null;
        // it is likely that fromNodeKey is the same as the last call because of the way the Wikipedia content is organised
        boolean fromNodeInsertIntoIndex = false;
        if (fromNodeKey.equals(lastFromNodeKey))
        {
            fromNode = lastFromNode;
        }
        else
        {
            // See if node exists using index
            fromNode = getVertexFromIndex(fromNodeKey);
            if (fromNode == null)
            {
                // New vertex - add to graph and index
                fromNode = graph.createVertex();
                fromNodeInsertIntoIndex = true;
            }
            lastFromNode = fromNode;
            lastFromNodeKey = fromNodeKey;
        }
        ODocument toNode = null;
        boolean toNodeInsertIntoIndex = false;
        // it is likely that toNodeKey is the same as the last call because of the way the Wikipedia content is organised
        if (toNodeKey.equals(lastToNodeKey))
        {
            toNode = lastToNode;
        }
        else
        {
            // See if node exists using index
            toNode = getVertexFromIndex(toNodeKey);
            if (toNode == null)
            {
                // New vertex - add to graph and index
                toNode = graph.createVertex();
                toNodeInsertIntoIndex = true;
            }
            lastToNode = toNode;
            lastToNodeKey = toNodeKey;
        }
        // Create the edge
        graph.createEdge(fromNode, toNode).save();
        if (fromNodeInsertIntoIndex)
        {
            index.storeVertex(fromNodeKey, fromNode);
        }
        if (toNodeInsertIntoIndex)
        {
            index.storeVertex(toNodeKey, toNode);
        }
    }

    private ODocument getVertexFromIndex(final String fromNodeKey)
    {
        return index.getVertex(fromNodeKey);
    }

    static interface OrientIndex
    {
        void storeVertex(String udk, ODocument vertex);

        ODocument getVertex(String udk);

        void close();
    }

    static class OrientInternalIndex implements OrientIndex
    {
        OGraphDatabase graph;
        private OIndex<OIdentifiable> index;

        public OrientInternalIndex(OGraphDatabase graph)
        {
            this.graph = graph;
            index = (OIndex<OIdentifiable>) graph
                    .getMetadata()
                    .getIndexManager()
                    .createIndex("idx", "UNIQUE", new OSimpleKeyIndexDefinition(OType.STRING),
                            null, null);
        }

        @Override
        public void storeVertex(String udk, ODocument vertex)
        {
            index.put(udk, vertex);
        }

        @Override
        public ODocument getVertex(String udk)
        {
            final OIdentifiable result = index.get(udk);
            if (result != null)
            {
                return (ODocument) result.getRecord();
            }
            return null;
        }

        @Override
        public void close()
        {
        }
    }

    //A faster index for OrientDB work
    static class OrientIndexOnKVStore implements OrientIndex
    {
        KVStore index;
        OGraphDatabase graph;

        public OrientIndexOnKVStore(KVStore index, OGraphDatabase graph)
        {
            super();
            this.index = index;
            this.graph = graph;
        }

        @Override
        public void storeVertex(String udk, ODocument vertex)
        {
            ORID recordId = vertex.getIdentity();
            byte[] valueBytes = recordId.toStream();
            try
            {
                index.put(udk.getBytes(), valueBytes);
            }
            catch (IOException e)
            {
                throw new RuntimeException("Error storing key " + udk, e);
            }
        }

        @Override
        public ODocument getVertex(String udk)
        {
            try
            {
                byte[] valueBytes = index.get(udk.getBytes());
                if (valueBytes != null)
                {
                    ORecordId orID = new ORecordId();
                    orID.fromStream(valueBytes);
                    ODocument result = graph.getRecord(orID);
                    //                    System.out.println("Found id=" + result.toJSON());
                    return result;
                }
                else
                {
                    return null;
                }
            }
            catch (IOException e)
            {
                throw new RuntimeException("Error storing key " + udk, e);
            }
        }

        @Override
        public void close()
        {
            try
            {
                index.close();
            }
            catch (IOException e)
            {
                throw new RuntimeException("Error closing index", e);
            }
        }
    }

    //Cached lookup from last call
    private String lastFromNodeKey;
    private ODocument lastFromNode;
    private String lastToNodeKey;
    private ODocument lastToNode;

    @Override
    public void close()
    {
        index.close();
        graph.commit();
        graph.close();
    }

    public void setOrientDbDirName(String orientDbDirName)
    {
        this.orientDbDirName = orientDbDirName;
    }

    public String getOrientDbDirName()
    {
        return orientDbDirName;
    }

    public void setOrientDbName(String orientDbName)
    {
        this.orientDbName = orientDbName;
    }

    public String getOrientDbName()
    {
        return orientDbName;
    }

    static public boolean deleteDirectory(File path)
    {
        if (path.exists())
        {
            File[] files = path.listFiles();
            for (int i = 0; i < files.length; i++)
            {
                if (files[i].isDirectory())
                {
                    deleteDirectory(files[i]);
                }
                else
                {
                    files[i].delete();
                }
            }
        }
        return (path.delete());
    }

    public boolean isUseInternalOrientIndex()
    {
        return useInternalOrientIndex;
    }

    public void setUseInternalOrientIndex(boolean useInternalOrientIndex)
    {
        this.useInternalOrientIndex = useInternalOrientIndex;
    }
}
