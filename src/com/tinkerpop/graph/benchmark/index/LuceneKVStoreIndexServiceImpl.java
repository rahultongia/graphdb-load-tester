package com.tinkerpop.graph.benchmark.index;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;

import org.apache.lucene.kvstore.KVStore;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.SimpleFSDirectory;

/**
 * Uses Lucene KVStore to store and retrieve vertex Ids keyed on a user-defined key. Uses: 1) A
 * bloom filter "to know what we don't know" 2) An LRU cache to remember commonly accessed values we
 * do know 3) Uses a buffer to accumulate uncommitted state Lucene takes care of the rest.
 * @author Mark
 */
public class LuceneKVStoreIndexServiceImpl implements SimpleKeyToNodeIdIndex
{
    private String path;
    private KVStore map;
    int numInsertsSinceLastCommit = 0;

    //    LRUCache<String, Long> hotKeyCache = new LRUCache<String, Long>(10000);
    public LuceneKVStoreIndexServiceImpl(String path)
    {
        this.path = path;
    }

    public void init()
    {
        try
        {
            long start = System.currentTimeMillis();
            File f = new File(path);
            //        Directory dir = FSDirectory.open(f);//Let Lucene decide
            //        Directory dir = new NIOFSDirectory(f, null);
            f.mkdirs();
            Directory dir = new SimpleFSDirectory(f);
            map = new KVStore(dir);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void put(String udk, long graphNodeId)
    {
        numInsertsSinceLastCommit++;
        if (numInsertsSinceLastCommit > 10000)
        {
            try
            {
                map.commit();
            }
            catch (IOException e)
            {
                throw new RuntimeException("Error committing", e);
            }
            numInsertsSinceLastCommit = 0;
        }
        //        hotKeyCache.put(udk, graphNodeId);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream daos = new DataOutputStream(baos);
        try
        {
            daos.writeLong(graphNodeId);
            daos.close();
            map.put(udk.getBytes(), baos.toByteArray());
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error storing value:", e);
        }
    }

    @Override
    public long getGraphNodeId(String udk)
    {
        //        Long cachedValue = hotKeyCache.get(udk);
        //        if (cachedValue != null)
        //        {
        //            return cachedValue;
        //        }
        byte[] value;
        try
        {
            value = map.get(udk.getBytes());
            if (value == null)
            {
                return -1;
            }
            ByteArrayInputStream bais = new ByteArrayInputStream(value);
            DataInputStream dais = new DataInputStream(bais);
            long result = dais.readLong();
            dais.close();
            return result;
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error retrieiving value:", e);
        }
    }

    @Override
    public void close()
    {
        try
        {
            map.close();
        }
        catch (IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    //    static class LRUCache<K, V> extends LinkedHashMap<K, V>
    //    {
    //        /**
    //         * Default value
    //         */
    //        private static final long serialVersionUID = 1L;
    //        private int mMaxEntries;
    //
    //        public LRUCache(int maxEntries)
    //        {
    //            // removeEldestEntry() is called after a put(). To allow maxEntries in
    //            // cache, capacity should be maxEntries + 1 (+1 for the entry which will
    //            // be removed). Load factor is taken as 1 because size is fixed. This is
    //            // less space efficient when very less entries are present, but there
    //            // will be no effect on time complexity for get(). The third parameter
    //            // in the base class constructor says that this map is access-order
    //            // oriented.
    //            super(maxEntries + 1, 1, true);
    //            mMaxEntries = maxEntries;
    //        }
    //
    //        @Override
    //        protected boolean removeEldestEntry(Map.Entry<K, V> eldest)
    //        {
    //            // After size exceeds max entries, this statement returns true and the
    //            // oldest value will be removed. Since this map is access oriented the
    //            // oldest value would be least recently used.
    //            return size() > mMaxEntries;
    //        }
    //    }
}
