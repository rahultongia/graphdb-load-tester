package org.apache.lucene.kvstore;

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
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.procedure.TIntIntProcedure;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.getopt.util.hash.MurmurHash;

//@formatter:off
/**
 * For a good overview of this service see http://www.slideshare.net/MarkHarwood/lucene-kvstore
 * 
 *  This class provides a fast implementation of a key/value store with near constant access times
 *  regardless of the number of keys. The cost of a read is always a maximum of a single disk seek.
 *
 *  Keys and related values are held on disk in segment files and a RAM-based map is used to 
 *  hold hashes of the keys and an int-based pointer to the relevant section of a segment file.
 *  
 *  Updates are always appended to the end of an "active" segment until it reaches a given size and
 *  then a new generation of segment file is created. The latest value for a given key is always 
 *  assumed to be held in the latest generation segment that holds a copy. Because old values are left
 *  orphaned a background cleanup thread is used to periodically merge the older read-only segments 
 *  into new combined segments without the out-dated values.  
 *  
 *  Occasionally, different keys will produce the same hashcode and so each entry in a segment file is
 *  effectively a list of key/value pairs which share the same hash. Any reads must therefore check the 
 *  stored keys for equality and any updates must append a new entry to the end of the active segment,
 *  with the new key/value and taking care to bring any other key/value pairs that share the same hash.
 *  
 *  A "segments" file similar to Lucene's segments file, is used to provide transactional semantics and
 *  acts as the definitive list of segment IDs that make up the store when a commit is performed. 
 *  As well as the list of read-only segments in service the segments file identifies the length of the
 *  active segment file when the commit was issued - the assumption being that records after this position in
 *  the file may be in a corrupt/unknown state. Upon opening the store, only content in the active segment 
 *  up to the last known commit position is used.
 *  
 *  For faster load-times the read-only segments can also store a "fastload" file which effectively contains
 *  only the key hashes and entry offsets required to be loaded into RAM. It is possible to load content from
 *  the segment file but this requires parsing key and value content and computing hashcodes, often for entries
 *  that are made obsolete by later updates in the file. This is why the "fastload" file can be significantly
 *  faster than parsing the segment file.
 *  
 *  
 *  
 *  @author MAHarwood
 *       
 * 
 */
//@formatter:on
public class KVStore
{
    Directory fsDir;
    static final String SEGMENTS_FILE_NAME = "bigmapSegments"; //$NON-NLS-1$
    static final String SEGMENTS_TEMP_FILE_NAME = "bigmapSegmentsTemp"; //$NON-NLS-1$
    static final String SEGMENT_FILE_NAME_PREFIX = "bigmapSeg"; //$NON-NLS-1$
    private static final int READ_BUFFER_SIZE = 1024 * 4;
    private static final int DEFAULT_SEGMENT_MAP_START_SIZE = 10000;
    int segmentMapStartSize = DEFAULT_SEGMENT_MAP_START_SIZE;
    //    private static final int MAX_SEGMENT_SIZE = 64 * 1024 * 1024;
    private static final int MAX_SEGMENT_SIZE = 64 * 1024 * 1024;
    private static final String SEPARATOR = "_"; //$NON-NLS-1$
    ReadOnlySegment[] readOnlySegments;
    ActiveSegment activeSegment;
    //Set this to a non-zero number to set the threshold for initiating background merges
    private int minNumberDeletedKeysForAMerge = -1;
    private KVStoreConfig config;

    // A read+write enabled segment (only one write-capable segment is enabled at any one time).
    static class ActiveSegment
    {
        Directory dir;
        String segmentFileName;
        private IndexOutput out;
        private IndexInput in;
        TIntIntHashMap ptrMap;
        volatile long outLastFlushedLength = 0;
        volatile long inReadableExtent = 0;
        private int segNum;
        LRUCache<Key, byte[]> hotKeyCache = null;

        //Creates a new ActiveSegment
        public ActiveSegment(Directory dir, String segmentFileName, int startPointerSize,
                int segNum, KVStoreConfig config) throws IOException
        {
            super();
            this.dir = dir;
            if (config.getKeyValueLRUCacheSize() > 0)
            {
                hotKeyCache = new LRUCache<Key, byte[]>(config.getKeyValueLRUCacheSize());
            }
            this.segNum = segNum;
            this.segmentFileName = segmentFileName;
            //            this.out = dir.createOutput(segmentFileName, IOContext.DEFAULT);//Lucene 4.0
            this.out = dir.createOutput(segmentFileName);
            out.writeVInt(SOFTWARE_VERSION);//header version number
            out.flush();
            ptrMap = new TIntIntHashMap(startPointerSize);
        }

        //Used when opening an existing Active Segment
        public ActiveSegment(Directory dir, IndexOutput out, String segmentFileName,
                TIntIntHashMap ptrMap, int segNum, KVStoreConfig config)
        {
            super();
            if (config.getKeyValueLRUCacheSize() > 0)
            {
                hotKeyCache = new LRUCache<Key, byte[]>(config.getKeyValueLRUCacheSize());
            }
            this.dir = dir;
            this.out = out;
            this.segmentFileName = segmentFileName;
            this.segNum = segNum;
            this.ptrMap = ptrMap;
        }

        //Gets the value for a given key on the active segment - synchronized to avoid contention with write activity
        public synchronized byte[] get(int keyHash, byte[] searchKey) throws IOException
        {
            //Hits a "hot" cache to avoid disk access for commonly accessed keys
            //Hotly updated keys are likely to sit at end of file (possibly in write buffer)
            //Without a hotKeyCache we require a flush of the output buffer and reopen of IndexInput 
            //before we can read recent values back so the cache is very valuable.
            //TODO consider moving the cache up to top-level (i.e. not just active segment)
            //to avoid reads on frequently-read-but-rarely-updated keys?
            // Maybe OS-level file caching mitigates those areas for us anyway?
            byte[] result = null;
            if (hotKeyCache != null)
            {
                hotKeyCache.get(new Key(keyHash, searchKey));
            }
            if (result == null)
            {
                result = noneCachedGet(keyHash, searchKey);
            }
            return result;
        }

        static class Key
        {
            int keyHash;
            byte[] keyBytes;

            public Key(int keyHash, byte[] keyBytes)
            {
                super();
                this.keyHash = keyHash;
                this.keyBytes = keyBytes;
            }

            @Override
            public int hashCode()
            {
                return keyHash;
            }

            @Override
            public boolean equals(Object obj)
            {
                Key other = (Key) obj;
                return arrayEquals(keyBytes, other.keyBytes);
            }
        }

        //Gets the value for a given key on the active segment - synchronized to avoid contention with write activity
        private synchronized byte[] noneCachedGet(int keyHash, byte[] searchKey) throws IOException
        {
            int kvPos = 0;
            synchronized (ptrMap)
            {
                kvPos = ptrMap.get(keyHash);
            }
            if (kvPos == ptrMap.getNoEntryKey())
            {
                return null;
            }
            checkCanReadFromThisPosition(kvPos);
            in.seek(kvPos);
            int numKeys = in.readVInt();
            for (int i = 0; i < numKeys; i++)
            {
                int keySize = in.readVInt();
                byte[] key = new byte[keySize];
                in.readBytes(key, 0, keySize);
                int valueSize = in.readVInt();
                if (arrayEquals(key, searchKey))
                {
                    //Key matches - read the value
                    byte[] value = new byte[valueSize];
                    in.readBytes(value, 0, valueSize);
                    return value;
                }
                else
                {
                    //Non-matching key - slip over value bytes
                    in.seek(in.getFilePointer() + valueSize);
                }
            }
            return null;
        }

        public void put(ReadOnlySegment[] persistedSegments, byte[] key, byte[] newValue)
                throws IOException
        {
            int keyHash = hashBytes(key);
            int ptr = 0;
            synchronized (ptrMap)
            {
                ptr = ptrMap.get(keyHash);
            }
            if (ptr == ptrMap.getNoEntryKey())
            {
                putEntryNewToThisSegment(persistedSegments, key, newValue, keyHash);
            }
            else
            {
                updateEntryInThisSegment(key, newValue, keyHash, ptr);
            }
            if (hotKeyCache != null)
            {
                hotKeyCache.put(new Key(keyHash, key), newValue);
            }
        }

        private void updateEntryInThisSegment(byte[] key, byte[] newValue, int keyHash, int ptr)
                throws IOException
        {
            //We have a hash collision or update in this segment - copy old content plus new content to end of byte buffer
            checkCanReadFromThisPosition(ptr);
            in.seek(ptr);
            int numKeys = in.readVInt();
            boolean keyIsNew = true;
            //merge old+new values into Byte array
            ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
            OutputStreamDataOutput tmpOut = new OutputStreamDataOutput(baos);
            for (int i = 0; i < numKeys; i++)
            {
                int oldKeySize = in.readVInt();
                byte[] oldKey = new byte[oldKeySize];
                in.readBytes(oldKey, 0, oldKeySize);
                int oldValueSize = in.readVInt();
                byte[] oldValueBytes = new byte[oldValueSize];
                in.readBytes(oldValueBytes, 0, oldValueSize);
                tmpOut.writeVInt(oldKeySize);
                tmpOut.writeBytes(oldKey, oldKeySize);
                if (arrayEquals(oldKey, key))
                {
                    keyIsNew = false;
                    tmpOut.writeVInt(newValue.length);
                    tmpOut.writeBytes(newValue, newValue.length);
                }
                else
                {
                    tmpOut.writeVInt(oldValueSize);
                    tmpOut.writeBytes(oldValueBytes, oldValueSize);
                }
            }
            if (keyIsNew)
            {
                numKeys++;
                tmpOut.writeVInt(key.length);
                tmpOut.writeBytes(key, key.length);
                tmpOut.writeVInt(newValue.length);
                tmpOut.writeBytes(newValue, newValue.length);
            }
            long pos = out.getFilePointer();
            out.writeVInt(numKeys);
            //write all the keys+values held in tmpOut
            tmpOut.close();
            byte[] revisedKVData = baos.toByteArray();
            out.writeBytes(revisedKVData, revisedKVData.length);
            ptrMap.put(keyHash, (int) pos);
        }

        //Brand new key (to this segment) - append to end of write buffer;
        private void putEntryNewToThisSegment(ReadOnlySegment[] persistedSegments, byte[] key,
                byte[] newValue, int keyHash) throws IOException
        {
            //First bring forward any collisions from prior segments
            int numKeysWithThisHash = 1;
            List<KVPair> collidedKeys = null;
            for (int i = persistedSegments.length - 1; i >= 0; i--)
            {
                collidedKeys = persistedSegments[i].getAllCollisions(keyHash, key);
                if (collidedKeys != null)
                {
                    numKeysWithThisHash += collidedKeys.size();
                    break;
                }
            }
            long pos = out.getFilePointer();
            out.writeVInt(numKeysWithThisHash);//num keys
            //Write out the new Key/value
            out.writeVInt(key.length);
            out.writeBytes(key, key.length);
            out.writeVInt(newValue.length);
            out.writeBytes(newValue, newValue.length);
            //Write any key collisions brought forward from the earlier segment
            if (collidedKeys != null)
            {
                for (KVPair kvPair : collidedKeys)
                {
                    out.writeVInt(kvPair.key.length);
                    out.writeBytes(kvPair.key, kvPair.key.length);
                    out.writeVInt(kvPair.value.length);
                    out.writeBytes(kvPair.value, kvPair.value.length);
                }
            }
            ptrMap.put(keyHash, (int) pos);
        }

        //ensures the write buffer is appropriately flushed and that
        //file reader has visibility of the selected area
        private synchronized void checkCanReadFromThisPosition(long readPos) throws IOException
        {
            if (readPos >= outLastFlushedLength)
            {
                flush(); //flush the write buffer
            }
            if (in == null)
            {
                //                this.in = dir.openInput(segmentFileName, IOContext.DEFAULT);//Lucene 4.0
                this.in = dir.openInput(segmentFileName);//Lucene 4.0
                inReadableExtent = outLastFlushedLength;
            }
            else
            {
                if (readPos >= inReadableExtent)
                {
                    //reopen the input file to see the latest content
                    in.close();
                    //                    this.in = dir.openInput(segmentFileName, IOContext.DEFAULT);//Lucene 4.0
                    this.in = dir.openInput(segmentFileName);
                    inReadableExtent = outLastFlushedLength;
                }
            }
        }

        private void flush() throws IOException
        {
            synchronized (out)
            {
                out.flush();
                outLastFlushedLength = out.length();
            }
        }

        public int length() throws IOException
        {
            synchronized (out)
            {
                return (int) out.getFilePointer();
            }
        }

        //Returns the length of the file after the fsync
        public long commit() throws IOException
        {
            synchronized (out)
            {
                flush();
                dir.sync(Collections.singleton(segmentFileName));
                return out.length();
            }
        }

        public ReadOnlySegment endWritingAndMakeReadOnlyAccessor() throws IOException
        {
            //Flush and fsync the new segment
            flush();
            dir.sync(Collections.singleton(segmentFileName));
            out.close();
            out = null;
            if (in != null)
            {
                in.close();
            }
            //Open the newly flushed segment as a read-only segment
            //            in = dir.openInput(segmentFileName, IOContext.DEFAULT);//Lucene 4.0
            in = dir.openInput(segmentFileName);
            ReadOnlySegment readOnlyResult = new ReadOnlySegment(ptrMap, in, segNum, dir);
            in = null;
            return readOnlyResult;
        }

        public void closeNoCommit() throws IOException
        {
            if (in != null)
            {
                in.close();
            }
            if (out != null)
            {
                out.close();
            }
        }
    }

    public KVStore(Directory dir) throws IOException
    {
        this(dir, null);
    }

    public KVStore(Directory dir, KVStoreConfig config) throws IOException
    {
        this.fsDir = dir;
        if (config == null)
        {
            //Run with default settings
            config = new KVStoreConfig();
        }
        this.config = config;
        //=================================
        // Open segments control file
        //TODO loop around this, deleting the latest generation of the segments file if it is in anyway corrupt
        String[] allFiles = fsDir.listAll();
        String maxSegmentFile = null;
        int latestCommitNum = -1;
        for (String filename : allFiles)
        {
            if (filename.startsWith(SEGMENTS_FILE_NAME))
            {
                String[] parts = filename.split(SEPARATOR);
                if (parts.length == 2)
                {
                    int commitNumber = Integer.parseInt(parts[1]);
                    if (commitNumber > latestCommitNum)
                    {
                        latestCommitNum = commitNumber;
                        maxSegmentFile = SEGMENTS_FILE_NAME + SEPARATOR + latestCommitNum;
                    }
                    latestCommitNum = Math.max(latestCommitNum, commitNumber);
                }
            }
        }
        if (maxSegmentFile != null)
        {
            openExistingSegments(maxSegmentFile, latestCommitNum);
        }
        else
        {
            createBlankSegmentsFile();
        }
    }

    //thread-unsafe method only used while opening the store (which is always single-threaded)
    private final boolean isloadedSoFar(int keyHash)
    {
        if (activeSegment.ptrMap.contains(keyHash))
        {
            return true;
        }
        //TODO maybe maintain a BloomFilter of loaded keys to speed up load times (and "get" times?)
        //because the time taken to test all segments for key ownership is directly
        //proportional to the number of segments already loaded. The BloomFilter 
        //allows us to "know what we don't have" quickly and save the cost of winding
        //through all segments.
        //Alternatively maintain a single hash->pointer map for all segments where the
        //pointer is a long not an int and can be decoded to split into a segment ID and
        // a file pointer within the segment.
        for (int i = readOnlySegments.length - 1; i >= 0; i--)
        {
            if (readOnlySegments[i] == null)
            {
                return false;
            }
            if (readOnlySegments[i].ptrMap.containsKey(keyHash))
            {
                return true;
            }
        }
        return false;
    }

    private static final int SOFTWARE_VERSION = 1;
    private int activeSegID;
    private long activeSegLastSavePoint;
    int storedSegIds[] = new int[0];
    private int latestCommitNumber;

    void openExistingSegments(String filename, int commitNumber)
    {
        this.latestCommitNumber = commitNumber;
        IndexInput segsIn = null;
        try
        {
            //            segsIn = fsDir.openInput(filename, IOContext.READONCE); //Lucene 4.0
            segsIn = fsDir.openInput(filename);
            int softwareVersion = segsIn.readInt();
            activeSegID = segsIn.readInt();
            activeSegLastSavePoint = segsIn.readLong();
            int numSegs = segsIn.readInt();
            storedSegIds = new int[numSegs];
            readOnlySegments = new ReadOnlySegment[numSegs];
            for (int i = 0; i < numSegs; i++)
            {
                storedSegIds[i] = segsIn.readInt();
            }
            segsIn.close();
            activeSegment = openActiveSegment();
            //================================
            //Load all of the segments held on disk
            //Load the segments most recent first so we can avoid loading pointers to redundant keys
            for (int i = numSegs - 1; i >= 0; i--)
            {
                //TODO this could potentially be multithreaded for faster loading.
                readOnlySegments[i] = getReadOnlySegment(storedSegIds[i], segmentMapStartSize, this);
                segmentMapStartSize = (int) (readOnlySegments[i].getNumberHashes() * 1.1f);
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error opening store using segments file " + filename, e);
        }
    }

    synchronized int getNextSegmentID()
    {
        int highest = activeSegID;
        for (int segmentID : storedSegIds)
        {
            highest = Math.max(highest, segmentID);
        }
        return highest + 1;
    }

    public ActiveSegment openActiveSegment() throws IOException
    {
        String activeSegmentFileName = SEGMENT_FILE_NAME_PREFIX + activeSegID;
        if (fsDir.fileExists(activeSegmentFileName))
        {
            System.out
                    .println("Converting old active Seg from start of file to last commit point at "
                            + activeSegLastSavePoint);
            //Need to copy last ActiveSeg to new file because a) the end of the file may contain half committed content and b)
            //Lucene file APIs don't let me open for output an already existing file.
            //            IndexInput oldActiveSeg = fsDir.openInput(activeSegmentFileName, new IOContext(
            //                    Context.MERGE)); //Lucene 4.0
            IndexInput oldActiveSeg = fsDir.openInput(activeSegmentFileName);
            oldActiveSeg.readVInt();//read header version number
            activeSegID = getNextSegmentID();
            String revisedActiveSegName = SEGMENT_FILE_NAME_PREFIX + activeSegID;
            //            IndexOutput truncatedActiveSeg = fsDir.createOutput(revisedActiveSegName,IOContext.DEFAULT); //Lucene 4.0
            IndexOutput truncatedActiveSeg = fsDir.createOutput(revisedActiveSegName);
            truncatedActiveSeg.writeVInt(SOFTWARE_VERSION);
            //for all of the content prior to the last marked commit point, copy to new output file
            TIntIntHashMap ptrMap = new TIntIntHashMap(DEFAULT_SEGMENT_MAP_START_SIZE);
            while (oldActiveSeg.getFilePointer() < activeSegLastSavePoint)
            {
                int filePos = (int) oldActiveSeg.getFilePointer();
                int numKeys = oldActiveSeg.readVInt();
                truncatedActiveSeg.writeVInt(numKeys);
                for (int i = 0; i < numKeys; i++)
                {
                    int keySize = oldActiveSeg.readVInt();
                    byte[] key = new byte[keySize];
                    oldActiveSeg.readBytes(key, 0, keySize);
                    if (i == 0)
                    {
                        int hash = hashBytes(key);
                        ptrMap.put(hash, filePos);
                    }
                    int valueSize = oldActiveSeg.readVInt();
                    byte[] value = new byte[valueSize];
                    oldActiveSeg.readBytes(value, 0, valueSize);
                    //write KV to new file
                    truncatedActiveSeg.writeVInt(keySize);
                    truncatedActiveSeg.writeBytes(key, keySize);
                    truncatedActiveSeg.writeVInt(valueSize);
                    truncatedActiveSeg.writeBytes(value, valueSize);
                }
            }
            oldActiveSeg.close();
            //flush new active segment
            truncatedActiveSeg.flush();
            fsDir.sync(Collections.singleton(revisedActiveSegName));
            saveNewCommitInfo(truncatedActiveSeg.getFilePointer());
            ActiveSegment revisedSeg = new ActiveSegment(fsDir, truncatedActiveSeg,
                    revisedActiveSegName, ptrMap, activeSegID, config);
            fsDir.deleteFile(activeSegmentFileName); //Now we have recreated the active seg minus any corruptions we can delete it
            return revisedSeg;
        }
        return new ActiveSegment(fsDir, activeSegmentFileName, DEFAULT_SEGMENT_MAP_START_SIZE,
                activeSegID, config);
    }

    public ReadOnlySegment getReadOnlySegment(int segID, int segmentMapStartSize, KVStore bigmap)
            throws IOException
    {
        String segmentFileName = SEGMENT_FILE_NAME_PREFIX + (segID);
        //        IndexInput segIn = fsDir.openInput(segmentFileName, IOContext.READ); //Lucene 4.0
        IndexInput segIn = fsDir.openInput(segmentFileName);
        return new ReadOnlySegment(segIn, segmentMapStartSize, segID, bigmap);
    }

    public int getNumberSegments()
    {
        return readOnlySegments.length;
    }

    void createBlankSegmentsFile() throws IOException
    {
        latestCommitNumber = 0;
        activeSegLastSavePoint = 0;
        activeSegID = 0;
        //segIds = new int[0];
        readOnlySegments = new ReadOnlySegment[0];
        String segFile = SEGMENTS_FILE_NAME + SEPARATOR + latestCommitNumber;
        IndexOutput segsOut = null;
        try
        {
            //            segsOut = fsDir.createOutput(segFile, IOContext.DEFAULT); //Lucene 4.0
            segsOut = fsDir.createOutput(segFile);
            segsOut.writeInt(SOFTWARE_VERSION);
            segsOut.writeInt(0);
            segsOut.writeLong(0);
            segsOut.writeInt(0);
        }
        finally
        {
            segsOut.flush();
            segsOut.close();
            fsDir.sync(Collections.singleton(segFile));
        }
        activeSegment = openActiveSegment();
    }

    //Saves a new generation of the "segments" file, deleting the previous one
    synchronized void saveNewCommitInfo(long newSavePointInActiveSegment)
    {
        activeSegLastSavePoint = newSavePointInActiveSegment;
        String oldSegmentsFilename = SEGMENTS_FILE_NAME + SEPARATOR + latestCommitNumber;
        latestCommitNumber++;
        String newSegmentsFilename = SEGMENTS_FILE_NAME + SEPARATOR + latestCommitNumber;
        IndexOutput segsOut = null;
        try
        {
            //            segsOut = fsDir.createOutput(newSegmentsFilename, IOContext.DEFAULT); //Lucene 4.0
            segsOut = fsDir.createOutput(newSegmentsFilename);
            segsOut.writeInt(SOFTWARE_VERSION);
            segsOut.writeInt(activeSegID);
            segsOut.writeLong(activeSegLastSavePoint);
            segsOut.writeInt(storedSegIds.length);
            for (int i = 0; i < storedSegIds.length; i++)
            {
                segsOut.writeInt(storedSegIds[i]);
            }
            segsOut.flush();
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error writing new segment file " + newSegmentsFilename, e);
        }
        finally
        {
            if (segsOut != null)
            {
                try
                {
                    segsOut.close();
                    fsDir.deleteFile(oldSegmentsFilename);
                    fsDir.sync(Collections.singleton(newSegmentsFilename));
                }
                catch (IOException e)
                {
                    throw new RuntimeException("Error committing segment file "
                            + newSegmentsFilename, e);
                }
            }
        }
    }

    //Needs to be synchronized for the swap-over to a newly merged set of segments
    synchronized void activateMergedSegment(ReadOnlySegment newSeg, ReadOnlySegment[] segsToMerge)
            throws IOException
    {
        int newNumSegs = (readOnlySegments.length + 1) - segsToMerge.length;
        ReadOnlySegment[] newSegsList = new ReadOnlySegment[newNumSegs];
        int firstSeg = segsToMerge[0].segNum;
        boolean mergedOutSegsFound = false;
        int newSegsIndex = 0;
        for (int oldSegsIndex = 0; oldSegsIndex < readOnlySegments.length; oldSegsIndex++)
        {
            if (!mergedOutSegsFound)
            {
                if (readOnlySegments[oldSegsIndex].segNum == firstSeg)
                {
                    newSegsList[newSegsIndex++] = newSeg;
                    mergedOutSegsFound = true;
                    oldSegsIndex += segsToMerge.length - 1;//skip past the merged out segments
                }
                else
                {
                    newSegsList[newSegsIndex++] = readOnlySegments[oldSegsIndex];
                }
            }
            else
            {
                newSegsList[newSegsIndex++] = readOnlySegments[oldSegsIndex];
            }
        }
        readOnlySegments = newSegsList;
        storedSegIds = new int[readOnlySegments.length];
        for (int i = 0; i < readOnlySegments.length; i++)
        {
            storedSegIds[i] = readOnlySegments[i].segNum;
        }
        commit();
        //Close orphaned segments and delete orphaned files - TODO possibly need to support the idea of retaining multiple commit points?
        new OrphanedSegmentTidierThread(segsToMerge).start();
    }

    class OrphanedSegmentTidierThread extends Thread
    {
        private ReadOnlySegment[] orphanedSegments;
        static final int GRACE_PERIOD_MILLISECONDS = 10000;

        public OrphanedSegmentTidierThread(ReadOnlySegment[] orphanedSegments)
        {
            this.orphanedSegments = orphanedSegments;
        }

        @Override
        public void run()
        {
            try
            {
                sleep(GRACE_PERIOD_MILLISECONDS);
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
            for (ReadOnlySegment orphanedSegment : orphanedSegments)
            {
                try
                {
                    orphanedSegment.delete(fsDir);
                }
                catch (IOException e)
                {
                    System.err.println("Failed to delete orphaned segment" + e);
                }
            }
        }
    }

    //Gets the value for a given key - not synchronized to allow for concurrency in reads
    public byte[] get(byte[] searchKey) throws IOException
    {
        int keyHash = hashBytes(searchKey);
        byte[] result = activeSegment.get(keyHash, searchKey);
        if (result != null)
        {
            return result;
        }
        //Not found in active segment - check previously stored segments
        //Get a thread-safe snapshot of active segments
        ReadOnlySegment storedSegmentsSet[] = getCurrentSetOfSegments();
        //Cycle through the generations, most recent first
        for (int i = storedSegmentsSet.length - 1; i >= 0; i--)
        {
            result = storedSegmentsSet[i].get(keyHash, searchKey);
            if (result != null)
            {
                return result;
            }
        }
        return null;
    }

    //synchronized for protection during swap-overs due to background merges or roll-overs
    private synchronized ReadOnlySegment[] getCurrentSetOfSegments()
    {
        return readOnlySegments;
    }

    public synchronized void commit() throws IOException
    {
        long activeSegmentLastCommitPosition = activeSegment.commit();
        saveNewCommitInfo(activeSegmentLastCommitPosition);
    }

    private static class KVPair
    {
        byte[] key;
        byte[] value;

        public KVPair(byte[] key, byte[] value)
        {
            super();
            this.key = key;
            this.value = value;
        }
    }

    //Need to synchronize on all puts because of possible contention while switching over to a new active segment
    public synchronized void put(byte[] key, byte[] newValue) throws IOException
    {
        if ((activeSegment.length() + ((key.length + newValue.length) * 2)) > MAX_SEGMENT_SIZE)
        {
            rollOverToNewSegmentAndCommit();
            maybeMerge();
        }
        activeSegment.put(readOnlySegments, key, newValue);
    }

    public synchronized void maybeMerge()
    {
        if (activeMerge != null)
        {
            //merge already active.
            return;
        }
        //Find the best run of segments ripe for merging - TODO maybe externalize this into a MergePolicy class
        //The choice of segments that are to be merged must be contiguous segments in order to preserve any 
        //key precedence in relation to non-merged segments
        int[] numberOfRedundantHashesInSegments = new int[readOnlySegments.length];
        int[] numberOfActiveHashesInSegments = new int[readOnlySegments.length];
        int maxNumHashesStoredInAnySegment = 0;
        int minNumActiveHashesStoredInAnySegment = Integer.MAX_VALUE;
        //gather stats about number of hashes in each segment.
        for (int i = 0; i < readOnlySegments.length; i++)
        {
            numberOfActiveHashesInSegments[i] = readOnlySegments[i].getNumberHashes();
            numberOfRedundantHashesInSegments[i] = readOnlySegments[i].getNumberRedundantSlots();
            maxNumHashesStoredInAnySegment = Math.max(maxNumHashesStoredInAnySegment,
                    numberOfActiveHashesInSegments[i] + numberOfRedundantHashesInSegments[i]);
            minNumActiveHashesStoredInAnySegment = Math.min(minNumActiveHashesStoredInAnySegment,
                    numberOfActiveHashesInSegments[i]);
        }
        //maxNumHashesFoundInSegment is used here as a guideline for achieving full occupancy in a segment
        int maxSlotsReclaimable = -1;
        int bestSegmentStart = 0;
        int bestSegmentEnd = 0;
        for (int i = 0; i < numberOfActiveHashesInSegments.length; i++)
        {
            //Find the potential space reclaimable, if segments are merged
            int newlyMergedSegmentSize = numberOfActiveHashesInSegments[i];
            int candidateSegmentStart = i;
            int candidateSegmentEnd = i;
            int slotsReclaimable = numberOfRedundantHashesInSegments[i];
            for (int j = i + 1; j < numberOfActiveHashesInSegments.length; j++)
            {
                int resultOfMergeSize = newlyMergedSegmentSize + numberOfActiveHashesInSegments[j];
                if (resultOfMergeSize < maxNumHashesStoredInAnySegment)
                {
                    newlyMergedSegmentSize = resultOfMergeSize;
                    candidateSegmentEnd = j;
                    slotsReclaimable += numberOfRedundantHashesInSegments[j];
                }
                else
                {
                    break;
                }
            }
            if (slotsReclaimable > maxSlotsReclaimable)
            {
                maxSlotsReclaimable = slotsReclaimable;
                bestSegmentStart = candidateSegmentStart;
                bestSegmentEnd = candidateSegmentEnd;
            }
        }
        int mergeThreshold = minNumberDeletedKeysForAMerge;
        if (mergeThreshold <= 0)
        {
            //Avoid the expense of merging unnecessarily - the space reclaimable must be at least as big
            // as the smallest number of active keys in any one segment
            mergeThreshold = minNumActiveHashesStoredInAnySegment;
        }
        if (maxSlotsReclaimable > mergeThreshold)
        {
            int numSegsToMerge = (bestSegmentEnd + 1) - bestSegmentStart;
            ReadOnlySegment[] segsToMerge = new ReadOnlySegment[numSegsToMerge];
            int m = 0;
            for (int i = bestSegmentStart; i <= bestSegmentEnd; i++)
            {
                segsToMerge[m++] = readOnlySegments[i];
            }
            activeMerge = new MergeThread(segsToMerge);
            activeMerge.start();
        }
        else
        {
            //            System.out.println("Not merging - maxReclaim=" + maxSlotsReclaimable
            //                    + " vs min actives=" + minNumActiveHashesStoredInAnySegment);
        }
    }

    MergeThread activeMerge = null;

    class MergeThread extends Thread
    {
        private ReadOnlySegment[] segsToMerge;

        public MergeThread(ReadOnlySegment[] segsToMerge)
        {
            this.segsToMerge = segsToMerge;
        }

        @Override
        public void run()
        {
            try
            {
                System.err.println("Merging!!!!!");
                int newMergedSegID = getNextSegmentID();
                String newCompactedSegFileName = SEGMENT_FILE_NAME_PREFIX + newMergedSegID;
                //                IndexOutput compactedNewSeg = fsDir.createOutput(newCompactedSegFileName,
                //                        new IOContext(Context.MERGE));//Lucene 4.0
                IndexOutput compactedNewSeg = fsDir.createOutput(newCompactedSegFileName);
                compactedNewSeg.writeVInt(1);//Software version
                long start = System.currentTimeMillis();
                TIntIntHashMap combinedMap = new TIntIntHashMap();
                //Combine all pointers into a single map which would leave us with a map containing the latest pointer for each hash
                for (int i = 0; i < segsToMerge.length; i++)
                {
                    synchronized (segsToMerge[i].ptrMap)
                    {
                        combinedMap.putAll(segsToMerge[i].ptrMap);
                    }
                }
                TIntIntHashMap revisedPointerMap = new TIntIntHashMap();
                for (int i = 0; i < segsToMerge.length; i++)
                {
                    //Clone the file to avoid any conflict with ongoing queries on that file
                    IndexInput in = (IndexInput) segsToMerge[i].segmentFile.clone();
                    in.seek(0);
                    int version = in.readVInt();
                    while (in.getFilePointer() < in.length())
                    {
                        long rowPos = in.getFilePointer();
                        int numKeys = in.readVInt();
                        boolean currentRowIsDeleted = false;
                        int keyHash = 0;
                        for (int k = 0; k < numKeys; k++)
                        {
                            int keySize = in.readVInt();
                            byte[] key = new byte[keySize];
                            in.readBytes(key, 0, keySize);
                            int valueSize = in.readVInt();
                            byte[] value = new byte[valueSize];
                            in.readBytes(value, 0, valueSize);
                            if (k == 0)
                            {
                                keyHash = hashBytes(key);
                                long ptrPos = combinedMap.get(keyHash);
                                if (ptrPos != rowPos) //Is the current row deleted?
                                {
                                    currentRowIsDeleted = true;
                                }
                            }
                            if (!currentRowIsDeleted)
                            {
                                long newRowPos = compactedNewSeg.getFilePointer();
                                //Key matches - read the value
                                if (k == 0)
                                {
                                    compactedNewSeg.writeVInt(numKeys);
                                }
                                revisedPointerMap.put(keyHash, (int) newRowPos);
                                compactedNewSeg.writeVInt(keySize);
                                compactedNewSeg.writeBytes(key, keySize);
                                compactedNewSeg.writeVInt(valueSize);
                                compactedNewSeg.writeBytes(value, valueSize);
                            }
                        }//end a row (a set of KV pairs with a common hash
                    }// end of file test
                    in.close();
                }
                compactedNewSeg.flush();
                compactedNewSeg.close();
                long diff = System.currentTimeMillis() - start;
                System.out.print("Took " + diff + " ms to compact segments ");
                for (ReadOnlySegment seg : segsToMerge)
                {
                    System.out.print(seg.segNum + ", ");
                }
                //                IndexInput newSegIn = fsDir.openInput(newCompactedSegFileName, IOContext.DEFAULT); //Lucene 4.0
                IndexInput newSegIn = fsDir.openInput(newCompactedSegFileName);
                ReadOnlySegment newSeg = new ReadOnlySegment(revisedPointerMap, newSegIn,
                        newMergedSegID, fsDir);
                activateMergedSegment(newSeg, segsToMerge);
                System.out.println(" into " + newCompactedSegFileName);
                //TODO rename the temp background file (TO WHAT?) and commit as new set of changes
            }
            catch (Exception e)
            {
                throw new RuntimeException("Error performing background merge", e);
            }
            finally
            {
                activeMerge = null;
            }
        }
    }

    private synchronized void rollOverToNewSegmentAndCommit() throws IOException
    {
        ReadOnlySegment newlyCompletedSegment = activeSegment.endWritingAndMakeReadOnlyAccessor();
        ReadOnlySegment[] newReadOnlySegments = new ReadOnlySegment[readOnlySegments.length + 1];
        for (int i = 0; i < readOnlySegments.length; i++)
        {
            newReadOnlySegments[i] = readOnlySegments[i];
        }
        newReadOnlySegments[readOnlySegments.length] = newlyCompletedSegment;
        readOnlySegments = newReadOnlySegments;
        storedSegIds = new int[readOnlySegments.length];
        for (int i = 0; i < readOnlySegments.length; i++)
        {
            storedSegIds[i] = readOnlySegments[i].segNum;
        }
        //        activeSegment = startNewSegmentAndCommitChanges();
        activeSegID = getNextSegmentID();
        activeSegLastSavePoint = 0;
        String activeSegName = SEGMENT_FILE_NAME_PREFIX + activeSegID;
        saveNewCommitInfo(activeSegLastSavePoint);
        activeSegment = new ActiveSegment(fsDir, activeSegName, DEFAULT_SEGMENT_MAP_START_SIZE,
                activeSegID, config);
    }

    //Represents a filled-to-capacity segment file of key-value pairs
    static class ReadOnlySegment
    {
        private static final String FAST_LOAD_FILE_SUFFIX = ".fastload";
        TIntIntMap ptrMap;
        IndexInput segmentFile;
        private int numRowsInThisSegMadeRedundantByLaterRecordsInThisSeg;
        int numRowsInThisSegMadeRedundantByNewerSegments;
        private int rows;
        private int segNum;

        public ReadOnlySegment(IndexInput segmentFile, int segmentMapStartSize, int segNum,
                KVStore bigmap) throws IOException
        {
            this.segNum = segNum;
            //When starting from cold need to parse files from disk.
            ptrMap = new TIntIntHashMap(DEFAULT_SEGMENT_MAP_START_SIZE);
            this.segmentFile = segmentFile;
            boolean isQuickLoaded = loadFromFastLoadFileIfExists(bigmap.fsDir);
            if (isQuickLoaded)
            {
                return;
            }
            //========= No fast-load file found - perform full-file-scan load of ptrs
            int segVersion = segmentFile.readVInt();
            int biggestValue = 0;
            //Read all "buckets" which are numKeysWithThisHash(vint) {vint keySize, keyBytes, vInt valSize, valBytes}            
            while (segmentFile.getFilePointer() < segmentFile.length())
            {
                long bucketPos = segmentFile.getFilePointer();
                int numKeysWithThisHash = segmentFile.readVInt();
                int keyHash = 0;
                for (int j = 0; j < numKeysWithThisHash; j++)
                {
                    int keyBufferSize = segmentFile.readVInt();
                    if (j == 0)
                    {
                        //read and hash the first key
                        byte[] keyBuffer = new byte[keyBufferSize];
                        segmentFile.readBytes(keyBuffer, 0, keyBufferSize);
                        keyHash = hashBytes(keyBuffer);
                        //If the keyhash is known to be superceded by a later segment 
                        //do not store a pointer to this outdated content in ram
                        if (!bigmap.isloadedSoFar(keyHash))
                        {
                            int oldPos = ptrMap.put(keyHash, (int) bucketPos);
                            if (oldPos != ptrMap.getNoEntryValue())
                            {
                                numRowsInThisSegMadeRedundantByLaterRecordsInThisSeg++;
                            }
                        }
                        else
                        {
                            numRowsInThisSegMadeRedundantByNewerSegments++;
                        }
                        rows++;
                    }
                    else
                    {
                        //skip over the key bytes TODO check this works OK - had some issues on the getCollisions method with this sort of skipping code
                        segmentFile.seek(segmentFile.getFilePointer() + keyBufferSize);
                    }
                    //skip over the value
                    int valueBufferSize = segmentFile.readVInt();
                    if (valueBufferSize > biggestValue)
                    {
                        biggestValue = valueBufferSize;
                    }
                    segmentFile.seek(segmentFile.getFilePointer() + valueBufferSize);
                }
            }
            createFastLoadFileIfNotExists(bigmap.fsDir);
            int totalRowsDeleted = getNumberRedundantSlots();
            int percentWasted = (int) (100f * (((float) totalRowsDeleted / (float) rows)));
            System.out.println("Segment " + segNum + " has " + totalRowsDeleted + " out of " + rows //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                    + " rows deleted (" + percentWasted + "%)"); //$NON-NLS-1$//$NON-NLS-2$
        }

        void delete(Directory fsDir) throws IOException
        {
            close();
            String thisFileName = SEGMENT_FILE_NAME_PREFIX + segNum;
            fsDir.deleteFile(thisFileName);
            String fastLoadFileName = segNum + FAST_LOAD_FILE_SUFFIX;
            if (fsDir.fileExists(fastLoadFileName))
            {
                fsDir.deleteFile(fastLoadFileName);
            }
            System.out.println("Deleting orphaned segment:" + thisFileName);
        }

        protected int getNumberRedundantSlots()
        {
            synchronized (ptrMap)
            {
                return numRowsInThisSegMadeRedundantByLaterRecordsInThisSeg
                        + numRowsInThisSegMadeRedundantByNewerSegments;
            }
        }

        //Create a file used for faster loads from cold (avoids reading all the key and values plus hashing)
        //TODO problem is that it may keep keys in RAM that have been superceded?
        private void createFastLoadFileIfNotExists(Directory dir) throws IOException
        {
            //TODO should ideally create a temp file then rename it only once completed but Directory.renameFile() no longer exists!
            String hintFileName = segNum + FAST_LOAD_FILE_SUFFIX;
            if (!dir.fileExists(hintFileName))
            {
                //                final IndexOutput out = dir.createOutput(hintFileName, IOContext.DEFAULT); //Lucene 4.0
                final IndexOutput out = dir.createOutput(hintFileName);
                //TODO this data is currently only ever updated here once but may need updating as we go along
                out.writeVInt(numRowsInThisSegMadeRedundantByLaterRecordsInThisSeg);
                out.writeVInt(numRowsInThisSegMadeRedundantByNewerSegments);
                try
                {
                    ptrMap.forEachEntry(new TIntIntProcedure() {
                        @Override
                        public boolean execute(int keyHash, int diskPtr)
                        {
                            try
                            {
                                out.writeVInt(keyHash);
                                out.writeVInt(diskPtr);
                            }
                            catch (Exception e)
                            {
                                throw new RuntimeException("Error writing hint file for " + segNum,
                                        e);
                            }
                            return true;
                        }
                    });
                    out.flush();
                }
                finally
                {
                    out.close();
                }
            }
        }

        private boolean loadFromFastLoadFileIfExists(Directory dir) throws IOException
        {
            String hintFileName = segNum + FAST_LOAD_FILE_SUFFIX;
            if (dir.fileExists(hintFileName))
            {
                //                final IndexInput in = dir.openInput(hintFileName, IOContext.DEFAULT); //Lucene 4.0
                final IndexInput in = dir.openInput(hintFileName);
                numRowsInThisSegMadeRedundantByLaterRecordsInThisSeg = in.readVInt();
                numRowsInThisSegMadeRedundantByNewerSegments = in.readVInt();
                try
                {
                    while (in.getFilePointer() < in.length())
                    {
                        int hash = in.readVInt();
                        int ptr = in.readVInt();
                        ptrMap.put(hash, ptr);
                    }
                }
                finally
                {
                    in.close();
                }
                return true;
            }
            else
            {
                return false;
            }
        }

        //Used to find and "pass forward" any collisions on a key hash. These collisions must subsequently be appended
        // to the active segment where the given key has just received an updated value.
        public List<KVPair> getAllCollisions(int keyHash, byte[] newKey) throws IOException
        {
            ArrayList<KVPair> collisions = null;
            int ptr = 0;
            synchronized (ptrMap)
            {
                ptr = ptrMap.get(keyHash);
            }
            if (ptr != ptrMap.getNoEntryValue())
            {
                //Remove keyHash from ptrMap here - it is being superceded by a new entry in the activeSegment
                synchronized (ptrMap)
                {
                    numRowsInThisSegMadeRedundantByNewerSegments++;
                    ptrMap.remove(keyHash); //TODO the previous get call could be replaced with examining response from this remove call
                }
                //Create a clone of the indexInput to avoid interfering with other thread's read activity
                IndexInput threadPrivateFile = (IndexInput) segmentFile.clone();
                try
                {
                    threadPrivateFile.seek(ptr);
                    int numKeysWithThisHash = threadPrivateFile.readVInt();
                    for (int j = 0; j < numKeysWithThisHash; j++)
                    {
                        int keyBufferSize = threadPrivateFile.readVInt();
                        byte[] keyBuffer = new byte[keyBufferSize];
                        threadPrivateFile.readBytes(keyBuffer, 0, keyBufferSize);
                        int valueBufferSize = threadPrivateFile.readVInt();
                        if (arrayEquals(keyBuffer, newKey))
                        {
                            //skip over the outdated value for the current key
                            threadPrivateFile.seek(threadPrivateFile.getFilePointer()
                                    + valueBufferSize);
                        }
                        else
                        {
                            //A collision that needs carrying over to the next segment
                            if (collisions == null)
                            {
                                collisions = new ArrayList<KVPair>(numKeysWithThisHash);
                            }
                            byte[] valueBuffer = new byte[valueBufferSize];
                            threadPrivateFile.readBytes(valueBuffer, 0, valueBufferSize);
                            KVPair collidedKVPair = new KVPair(keyBuffer, valueBuffer);
                            collisions.add(collidedKVPair);
                        }
                    }
                }
                finally
                {
                    threadPrivateFile.close();
                }
            }
            return collisions;
        }

        //This constructor is used with output from a newly flushed file.
        public ReadOnlySegment(TIntIntHashMap newPtrMap, IndexInput newSegIn, int segNum,
                Directory fsDir) throws IOException
        {
            this.ptrMap = newPtrMap;
            this.segmentFile = newSegIn;
            this.segNum = segNum;
            //As an optional speed-up for reloading from cold, flush a "hint file" which is faster to read than the
            //main file because that is filled with all of the keys+values, some of which may be obsolete and 
            //none of which are required in RAM
            createFastLoadFileIfNotExists(fsDir);
        }

        protected int getNumberHashes()
        {
            synchronized (ptrMap)
            {
                return ptrMap.size();
            }
        }

        //Not synchronized at this level to allow concurrency in get operations
        protected byte[] get(int keyHash, byte[] searchKey) throws IOException
        {
            int pointer = 0;
            synchronized (ptrMap)
            {
                pointer = ptrMap.get(keyHash);
            }
            if (pointer == ptrMap.getNoEntryValue())
            {
                return null;
            }
            //Clone the IndexInput so that parallel requests can read from the same file without interfering with each other's positions 
            IndexInput threadPrivateFile = (IndexInput) segmentFile.clone();
            try
            {
                threadPrivateFile.seek(pointer);
                int numKeysWithThisHash = threadPrivateFile.readVInt();
                for (int j = 0; j < numKeysWithThisHash; j++)
                {
                    int keyBufferSize = threadPrivateFile.readVInt();
                    byte[] keyBuffer = new byte[keyBufferSize];
                    threadPrivateFile.readBytes(keyBuffer, 0, keyBufferSize);
                    int valueBufferSize = threadPrivateFile.readVInt();
                    byte[] valueBuffer = new byte[valueBufferSize];
                    threadPrivateFile.readBytes(valueBuffer, 0, valueBufferSize);
                    if (arrayEquals(keyBuffer, searchKey))
                    {
                        //Found a match - return the associated value
                        return valueBuffer;
                    }
                }
            }
            finally
            {
                threadPrivateFile.close();
            }
            //No matches on the key
            return null;
        }

        public void close() throws IOException
        {
            segmentFile.close();
        }
    }

    private static boolean arrayEquals(byte[] a, byte[] a2)
    {
        int length = a.length;
        if (a2.length != length)
        {
            return false;
        }
        for (int i = 0; i < length; i++)
        {
            if (a[i] != a2[i])
            {
                return false;
            }
        }
        return true;
    }

    private static int hashBytes(byte[] data)
    {
        return MurmurHash.hash32(data);
    }

    public synchronized void close() throws IOException
    {
        commit();
        fsDir.close();
    }

    public synchronized void clear() throws IOException
    {
        if (activeSegment != null)
        {
            activeSegment.closeNoCommit();
        }
        for (ReadOnlySegment segment : readOnlySegments)
        {
            segment.close();
        }
        //Delete all files
        String[] names = fsDir.listAll();
        for (String name : names)
        {
            fsDir.deleteFile(name);
        }
        readOnlySegments = new ReadOnlySegment[0];
        createBlankSegmentsFile();
    }

    static class LRUCache<K, V> extends LinkedHashMap<K, V>
    {
        /**
         * Default value
         */
        private static final long serialVersionUID = 1L;
        private int mMaxEntries;

        public LRUCache(int maxEntries)
        {
            // removeEldestEntry() is called after a put(). To allow maxEntries in
            // cache, capacity should be maxEntries + 1 (+1 for the entry which will
            // be removed). Load factor is taken as 1 because size is fixed. This is
            // less space efficient when very less entries are present, but there
            // will be no effect on time complexity for get(). The third parameter
            // in the base class constructor says that this map is access-order
            // oriented.
            super(maxEntries + 1, 1, true);
            mMaxEntries = maxEntries;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<K, V> eldest)
        {
            // After size exceeds max entries, this statement returns true and the
            // oldest value will be removed. Since this map is access oriented the
            // oldest value would be least recently used.
            return size() > mMaxEntries;
        }
    }
}
