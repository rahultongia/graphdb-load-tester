package com.tinkerpop.graph.benchmark.loader;

import java.io.File;
import java.util.Iterator;

import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.tinkerpop.blueprints.pgm.Index;
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.TransactionalGraph.Conclusion;
import com.tinkerpop.blueprints.pgm.TransactionalGraph.Mode;
import com.tinkerpop.blueprints.pgm.impls.orientdb.OrientGraph;

/**
 * A {@link GraphLoaderService} that uses OrientDB via the Tinkerpop Blueprints abstraction
 * 
 * @author MAHarwood
 *
 */
public class TinkerpopOrientDbLoaderImpl implements GraphLoaderService
{
	
	private static final String USER_DEFINED_PRIMARY_KEY = "udk";
	private String orientDbDirName;
	private String orientDbName;
	int numInserts=0;
	private int maxNumRecordsBeforeCommit=100000;
	private OrientGraph graph;
	private Index<Vertex> index;
	
	//See http://code.google.com/p/orient/wiki/PerformanceTuning#Massive_Insertion
	//Not sure I beleive the statement in the above that says no transactions (and therefore autocommmit?) is faster than
	// carefully batched activity
	boolean useTransaction=true; 
	

	public TinkerpopOrientDbLoaderImpl ()  
	{

	}
	public void init()
	{
		File dir = new File(getOrientDbDirName());
		deleteDirectory(dir);
		dir.mkdirs();
		graph=new OrientGraph("local:"+getOrientDbDirName()+"/"+orientDbName);
		graph.getRawGraph().declareIntent( new OIntentMassiveInsert() );
		if(useTransaction)
		{
			graph.setTransactionMode(Mode.MANUAL);
			graph.startTransaction();
		}
		index=graph.createManualIndex("userKeyIndex", Vertex.class);		
		
	}
	

	@Override
	public void addLink(String fromNodeKey, String toNodeKey)
	{		
		Vertex fromNode=null;
		//it is likely that fromNodeKey is the same as the last call because of the way the Wikipedia content is organised
		if(fromNodeKey.equals(lastFromNodeKey))
		{
			fromNode=lastFromNode;
		}
		else
		{
			//See if node exists using index
			fromNode=getVertexFromIndex(fromNodeKey);
			if(fromNode==null)
			{
				//New vertex - add to graph and index
				fromNode = graph.addVertex(null);
				numInserts++;
				index.put(USER_DEFINED_PRIMARY_KEY, fromNodeKey, fromNode);				
			}
			lastFromNode=fromNode;
			lastFromNodeKey=fromNodeKey;
		}
		
		Vertex toNode=null;
		//it is likely that toNodeKey is the same as the last call because of the way the Wikipedia content is organised
		if(toNodeKey.equals(lastToNodeKey))
		{
			toNode=lastToNode;
		}
		else
		{
			//See if node exists using index
			toNode=getVertexFromIndex(toNodeKey);
			if(toNode==null)
			{
				//New vertex - add to graph and index
				toNode = graph.addVertex(null);
				numInserts++;
				index.put(USER_DEFINED_PRIMARY_KEY, toNodeKey, toNode);				
			}
			lastToNode=toNode;
			lastToNodeKey=toNodeKey;
		}
		//Create edge
		graph.addEdge(null, fromNode, toNode, null);

		if(useTransaction)
		{
			if(numInserts>maxNumRecordsBeforeCommit)
			{
				graph.stopTransaction(Conclusion.SUCCESS);
				graph.startTransaction();	
				numInserts=0;
			}
		}
	}
	private Vertex getVertexFromIndex(String fromNodeKey)
	{
		Iterable<Vertex> results = index.get(USER_DEFINED_PRIMARY_KEY, fromNodeKey);
		Iterator<Vertex> iterator = results.iterator();
		if(iterator.hasNext())
		{
			return iterator.next();
		}
		return null;
	}
	//Cached lookup from last call
	private String lastFromNodeKey;
	private Vertex lastFromNode;
	private String lastToNodeKey;
	private Vertex lastToNode;

	@Override
	public void close()
	{
		if(useTransaction)
		{
			graph.stopTransaction(Conclusion.SUCCESS);
		}
		graph.shutdown();
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
				} else
				{
					files[i].delete();
				}
			}
		}
		return (path.delete());
	}
	public void setMaxNumRecordsBeforeCommit(int maxNumRecordsBeforeCommit)
	{
		this.maxNumRecordsBeforeCommit = maxNumRecordsBeforeCommit;
	}
	public int getMaxNumRecordsBeforeCommit()
	{
		return maxNumRecordsBeforeCommit;
	}
	public boolean isUseTransaction()
	{
		return useTransaction;
	}
	public void setUseTransaction(boolean useTransaction)
	{
		this.useTransaction = useTransaction;
	}
	

}
