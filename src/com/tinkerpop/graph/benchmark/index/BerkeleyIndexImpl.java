package com.tinkerpop.graph.benchmark.index;

import java.io.File;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

/**
 * For monitoring help see file:///D:/javalibs/berkeleyDb/je-4.1.7/docs/jconsole/JConsole-plugin.html
 * 
 * The code in this class was copied from "Hello World" type examples for Berkeley DB and
 * could probably benefit from some tuning of appropriate settings
 * 
 * 
 * @author MAHarwood
 *
 */
public class BerkeleyIndexImpl implements SimpleKeyToNodeIdIndex
{
	
	private Environment exampleEnv;
	private Database myDatabase;

	public BerkeleyIndexImpl(String indexDir)
	{
		File f=new File(indexDir);
		deleteDirectory(f);
		f.mkdir();
		
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setTransactional(false);
        envConfig.setAllowCreate(true);
        exampleEnv = new Environment(new File(indexDir), envConfig);
        
        /*
         * Make a database within that environment
         *
         * Notice that we use an explicit transaction to
         * perform this database open, and that we
         * immediately commit the transaction once the
         * database is opened. This is required if we
         * want transactional support for the database.
         * However, we could have used autocommit to
         * perform the same thing by simply passing a
         * null txn handle to openDatabase().
         */        
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(false);
        dbConfig.setAllowCreate(true);
        dbConfig.setSortedDuplicates(true);
        myDatabase = exampleEnv.openDatabase(null,
                                                     "simpleDb",
                                                     dbConfig);
		
	}

	@Override
	public void close()
	{
		myDatabase.close();
		exampleEnv.close();
	}

	
	static public boolean deleteDirectory(File path)
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
		return (path.delete());
	}	
	long numGets=0;
	
	@Override
	public long getGraphNodeId(String aKey)
	{
		numGets++;

		try 
		{
		    // Need a key for the get
		    DatabaseEntry theKey = new DatabaseEntry(aKey.getBytes("UTF-8"));		    
		    // Need a DatabaseEntry to hold the associated data.
		    DatabaseEntry theData = new DatabaseEntry();
		    // Bindings need only be created once for a given scope
		    EntryBinding<Long> myBinding = TupleBinding.getPrimitiveBinding(Long.class);
		    // Get it
		    OperationStatus retVal = myDatabase.get(null, theKey, theData, 
		                                            LockMode.DEFAULT);
		    if (retVal == OperationStatus.SUCCESS) 
		    {
		        // Recreate the data.
		        // Use the binding to convert the byte array contained in theData
		        // to a Long type.
		        return  myBinding.entryToObject(theData);
		    } 
		    else 
		    {
		        return -1;
		    }
		} 
		catch (Exception e) 
		{
		    throw new RuntimeException(e);
		}
	}
	

	@Override
	public void put(String aKey, long graphNodeId)
	{
		try 
		{
		    DatabaseEntry theKey = new DatabaseEntry(aKey.getBytes("UTF-8"));    
		    // Now build the DatabaseEntry using a TupleBinding
		    DatabaseEntry theData = new DatabaseEntry();
		    EntryBinding<Long> myBinding = TupleBinding.getPrimitiveBinding(Long.class);
		    myBinding.objectToEntry(graphNodeId, theData);
		    // Now store it
		    myDatabase.put(null, theKey, theData);
		} 
		catch (Exception e) 
		{
		    throw new RuntimeException(e);
		}
	}
}