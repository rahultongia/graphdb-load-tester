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

import krati.core.StoreConfig;
import krati.core.StoreFactory;
import krati.core.segment.ChannelSegmentFactory;
import krati.core.segment.MappedSegmentFactory;
import krati.core.segment.MemorySegmentFactory;
import krati.store.DataStore;

/**
 * Uses Krati to store and retrieve vertex Ids keyed on a user-defined key.
 * @author Mark
 */
public class KratiIndexServiceImpl implements SimpleKeyToNodeIdIndex
{
    public static final String MEMORY_SEGMENT_TYPE = "Memory";
    public static final String MAPPED_SEGMENT_TYPE = "Mapped";
    public static final String CHANNEL_SEGMENT_TYPE = "Channel";
    public static final int DEFAULT_SEGMENT_FILE_SIZE_MB = 64;
    private String path;
    int numInsertsSinceLastCommit = 0;
    int initialCapacity = 10000000;
    private DataStore<byte[], byte[]> map;
    String segmentType = MEMORY_SEGMENT_TYPE;
    private int segmentFileSizeMB = DEFAULT_SEGMENT_FILE_SIZE_MB;
    public int DEFAULT_NUM_SEGMENTS_BETWEEN_COMMITS = 10000;
    private int numInsertsBetweenCommits = DEFAULT_NUM_SEGMENTS_BETWEEN_COMMITS;

    public KratiIndexServiceImpl(String path)
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
            map = createDataStore(f, initialCapacity);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a DataStore instance.
     * <p>
     * Subclasses can override this method to provide a specific DataStore implementation such as
     * DynamicDataStore and IndexedDataStore or provide a specific SegmentFactory such as
     * ChannelSegmentFactory, MappedSegmentFactory and WriteBufferSegment.
     */
    protected DataStore<byte[], byte[]> createDataStore(File homeDir, int initialCapacity)
            throws Exception
    {
        StoreConfig config = new StoreConfig(homeDir, initialCapacity);
        if (segmentType.equals(MEMORY_SEGMENT_TYPE))
        {
            config.setSegmentFactory(new MemorySegmentFactory());
        }
        else if (segmentType.equals(MAPPED_SEGMENT_TYPE))
        {
            config.setSegmentFactory(new MappedSegmentFactory());
        }
        else if (segmentType.equals(CHANNEL_SEGMENT_TYPE))
        {
            config.setSegmentFactory(new ChannelSegmentFactory());
        }
        else
        {
            throw new IllegalStateException("Invalid segment type of \"" + segmentType + " "
                    + "Please use " + MEMORY_SEGMENT_TYPE + " or " + MAPPED_SEGMENT_TYPE + " or "
                    + CHANNEL_SEGMENT_TYPE);
        }
        config.setSegmentFileSizeMB(segmentFileSizeMB);
        //TODO see discussion here on tuning: http://groups.google.com/group/krati/browse_thread/thread/fbc445367da4430f
        //And here for design http://sna-projects.com/krati/design.php
        //        return StoreFactory.createStaticDataStore(config);
        return StoreFactory.createDynamicDataStore(config);
    }

    @Override
    public void put(String udk, long graphNodeId)
    {
        numInsertsSinceLastCommit++;
        if (numInsertsSinceLastCommit > numInsertsBetweenCommits)
        {
            try
            {
                map.sync();
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

    public int getInitialCapacity()
    {
        return initialCapacity;
    }

    public void setInitialCapacity(int initialCapacity)
    {
        this.initialCapacity = initialCapacity;
    }

    public String getSegmentType()
    {
        return segmentType;
    }

    public void setSegmentType(String segmentType)
    {
        this.segmentType = segmentType;
    }

    public int getSegmentFileSizeMB()
    {
        return segmentFileSizeMB;
    }

    public void setSegmentFileSizeMB(int segmentFileSizeMB)
    {
        this.segmentFileSizeMB = segmentFileSizeMB;
    }

    public int getNumInsertsBetweenCommits()
    {
        return numInsertsBetweenCommits;
    }

    public void setNumInsertsBetweenCommits(int numInsertsBetweenCommits)
    {
        this.numInsertsBetweenCommits = numInsertsBetweenCommits;
    }
}
