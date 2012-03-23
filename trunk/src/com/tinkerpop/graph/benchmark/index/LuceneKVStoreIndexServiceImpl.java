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
 * Uses Lucene KVStore to store and retrieve vertex Ids keyed on a user-defined key.
 * @author Mark
 */
public class LuceneKVStoreIndexServiceImpl implements SimpleKeyToNodeIdIndex
{
    private String path;
    private KVStore map;
    int numInsertsSinceLastCommit = 0;

    public LuceneKVStoreIndexServiceImpl(String path)
    {
        this.path = path;
    }

    public void init()
    {
        try
        {
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
}
