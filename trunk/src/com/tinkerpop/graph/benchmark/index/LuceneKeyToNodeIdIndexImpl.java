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
import java.io.File;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.OpenBitSet;

/**
 * Uses Lucene to store and retrieve vertex Ids keyed on a user-defined key.
 * Uses:
 * 1) A bloom filter "to know what we don't know"
 * 2) An LRU cache to remember commonly accessed values we do know
 * 3) Uses a buffer to accumulate uncommitted state
 * 
 * Lucene takes care of the rest.
 * 
 * @author Mark
 *
 */
public class LuceneKeyToNodeIdIndexImpl implements SimpleKeyToNodeIdIndex
{
	LRUCache<String,Long>hotCache;
	HashMap<String, Long>uncommittedKeyBuffer=new HashMap<String, Long>();
	private IndexWriter writer;
	private FSDirectory dir;
	private IndexReader reader;
	Term term=new Term("udk","");
	
	
	int bloomFilterSize=50*1024*1024;
	int maxNumRecordsBeforeCommit=500000;
	private int lruCacheSize=500000;
	OpenBitSet bloomFilter;
	//Avoid Lucene performing "mega merges" with a finite limit on segments sizes that can be merged
	private int maxMergeMB=3000;
	
	//Stats for each batch of updates
	long bloomReadSaves=0;
	long hotCacheHits=0;
	long luceneAdds=0;
	long failedLuceneReads=0;
	long successfulLuceneReads=0;
	long startTime=System.currentTimeMillis();
	
	private IndexReader[] subreaders;
	private boolean showDebug;
	private String path;
	private boolean useCompoundFile=false;
	private double ramBufferSizeMB=300;
	private int termIndexIntervalSize=512;
	
	public LuceneKeyToNodeIdIndexImpl(String path, boolean showDebugInfo)
	{
		this.showDebug=showDebugInfo;
		this.path=path;
	}
	public void init()
	{
		try
		{
			hotCache=new LRUCache<String, Long>(getLruCacheSize());
			if(showDebug)
			System.out.println("timeTaken,numDocs,bloomReadSaves,hotCacheHits,failedLuceneReads,successfulLuceneReads,luceneAdds");
			bloomFilter=new OpenBitSet(bloomFilterSize);					
			deleteDirectoryContents(new File(path));			
			
			dir=FSDirectory.open(new File(path));
			writer=new IndexWriter(dir,new WhitespaceAnalyzer(),MaxFieldLength.UNLIMITED);
			LogByteSizeMergePolicy mp=new LogByteSizeMergePolicy(writer);
			mp.setMaxMergeMB(getMaxMergeMB());
			writer.setMergePolicy(mp);			
			writer.setUseCompoundFile(useCompoundFile);
			writer.setRAMBufferSizeMB(ramBufferSizeMB);
			writer.setTermIndexInterval(termIndexIntervalSize);
		} catch (Exception e)
		{
			throw new RuntimeException(e);
		}
		
	}

	static public void deleteDirectoryContents(File path)
	{
		if (path.exists())
		{
			File[] files = path.listFiles();
			for (int i = 0; i < files.length; i++)
			{
				if (!files[i].isDirectory())
				{
					files[i].delete();
				}
			}
		}
	}

	@Override
	public long getGraphNodeId(String udk)
	{
		Long result=hotCache.get(udk);
		if(result==null)
		{
			//fail fast on bloom
			int bloomKey=Math.abs(udk.hashCode()%bloomFilterSize);
			if(!bloomFilter.fastGet(bloomKey)){
				//Not seen - fail
				bloomReadSaves++;
				return -1;
			}
			
			result=uncommittedKeyBuffer.get(udk);
			if(result!=null)
			{
				return result;
			}
			
			
			if(reader==null)
			{
				try
				{
					reader=IndexReader.open(dir,true);
					subreaders=reader.getSequentialSubReaders();
				} catch (Exception e)
				{
					throw new RuntimeException(e);
				}
			}
			try
			{
				Term searchTerm = term.createTerm(udk);
				for (IndexReader r : subreaders)
				{
					TermDocs td = r.termDocs(searchTerm);
					if(td.next())
					{
						Document doc=r.document(td.doc());
						result=Long.parseLong(doc.get("id"));
						hotCache.put(udk, result);					
						successfulLuceneReads++;
						return result;
					}
					
				}
				failedLuceneReads++;
			} catch (Exception e)
			{
				throw new RuntimeException(e);
			}

		}
		else
		{
			hotCacheHits++;
		}
		if(result==null)
			return -1;
		else
			return result;
	}

	@Override
	public void put(String udk, long graphNodeId)
	{
		try
		{
			if(uncommittedKeyBuffer.size()>maxNumRecordsBeforeCommit)
			{
				writer.commit();
				if(reader!=null)
				{
					IndexReader newReader = reader.reopen(true);
					if(newReader!=reader)
					{
						reader.close();
						reader=newReader;
					}
					subreaders=reader.getSequentialSubReaders();
					
				}
				uncommittedKeyBuffer.clear();
				
				if(showDebug)
				{
					long diff=System.currentTimeMillis()-startTime;
					System.out.println(diff+","+reader.maxDoc()+","+bloomReadSaves+","+hotCacheHits+","+failedLuceneReads+","+successfulLuceneReads+","+luceneAdds);
				}
				
				bloomReadSaves=0;
				hotCacheHits=0;
				failedLuceneReads=0;
				luceneAdds=0;
				successfulLuceneReads=0;
				startTime=System.currentTimeMillis();
				
				
			}
			int bloomKey=Math.abs(udk.hashCode()%bloomFilterSize);
			bloomFilter.fastSet(bloomKey);
			uncommittedKeyBuffer.put(udk, graphNodeId);
			Document doc=new Document();
			Field udkF = new Field("udk",udk,Field.Store.NO,Field.Index.NOT_ANALYZED_NO_NORMS);
			udkF.setOmitTermFreqAndPositions(true);
			doc.add(udkF);
			doc.add(new Field("id",""+graphNodeId,Field.Store.YES,Field.Index.NO));
			writer.addDocument(doc);
			luceneAdds++;
		}
		catch(Exception e)
		{
			throw new RuntimeException("Error adding key to index",e);
		}
	}

	@Override
	public void close()
	{
		try
		{
			reader.close();
			writer.close();
			dir.close();
		}
		catch(Exception e)
		{
			throw new RuntimeException(e);
		}
		
	}
	
	@SuppressWarnings("serial")
	static class LRUCache <K,V> extends LinkedHashMap<K, V>
	{
		private int maxSize;

		public LRUCache(int maxSize)
		{
			super(maxSize * 4/3+1,0.75f, true);
			this.maxSize=maxSize;
		}

		@Override
		protected boolean removeEldestEntry(java.util.Map.Entry<K, V> eldest)
		{
			return size()>maxSize;
		}	
		
	}

	public boolean isUseCompoundFile()
	{
		return useCompoundFile;
	}
	public void setUseCompoundFile(boolean useCompoundFile)
	{
		this.useCompoundFile = useCompoundFile;
	}
	public double getRamBufferSizeMB()
	{
		return ramBufferSizeMB;
	}
	public void setRamBufferSizeMB(double ramBufferSizeMB)
	{
		this.ramBufferSizeMB = ramBufferSizeMB;
	}
	public int getTermIndexIntervalSize()
	{
		return termIndexIntervalSize;
	}
	public void setTermIndexIntervalSize(int termIndexIntervalSize)
	{
		this.termIndexIntervalSize = termIndexIntervalSize;
	}
	public int getBloomFilterSize()
	{
		return bloomFilterSize;
	}
	public void setBloomFilterSize(int bloomFilterSize)
	{
		this.bloomFilterSize = bloomFilterSize;
	}
	public int getMaxNumRecordsBeforeCommit()
	{
		return maxNumRecordsBeforeCommit;
	}
	public void setMaxNumRecordsBeforeCommit(int maxNumRecordsBeforeCommit)
	{
		this.maxNumRecordsBeforeCommit = maxNumRecordsBeforeCommit;
	}
	public void setLruCacheSize(int lruCacheSize)
	{
		this.lruCacheSize = lruCacheSize;
	}
	public int getLruCacheSize()
	{
		return lruCacheSize;
	}
	public void setMaxMergeMB(int maxMergeMB)
	{
		this.maxMergeMB = maxMergeMB;
	}
	public int getMaxMergeMB()
	{
		return maxMergeMB;
	}
	public long getBloomReadSaves()
	{
		return bloomReadSaves;
	}
	public long getHotCacheHits()
	{
		return hotCacheHits;
	}
	public long getLuceneAdds()
	{
		return luceneAdds;
	}
	public long getFailedLuceneReads()
	{
		return failedLuceneReads;
	}
	public long getSuccessfulLuceneReads()
	{
		return successfulLuceneReads;
	}	

}
