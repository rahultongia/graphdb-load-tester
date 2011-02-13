package com.tinkerpop.graph.benchmark;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.tools.bzip2.CBZip2InputStream;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.core.io.FileSystemResource;

import com.tinkerpop.graph.benchmark.loader.GraphLoaderService;

/**
 * Test framework to load a very large graph ( Wikipedia links - download from http://downloads.dbpedia.org/3.6/en/page_links_en.nt.bz2 )
 * Outputs a benchmark file showing insert costs over time which can hopefully can be published and compared.
 * 
 * A choice of Spring config file is used to initialize the choice of graph database implementation. 
 * 
 * @author MAHarwood
 *
 */
public class TestBatchGraphLoad
{
	
	String benchmarkResultsFilename;
	String inputTestDataFile;
	//Log progress every n records...
	int batchReportTime=100000;
	//The name of a file which if found will stop the ingest at the next batch progress report time
	String stopFileName="stop.txt";	
	int numRecordsToLoad=-1; //set to a value >0 to limit the scope of the test
	
	GraphLoaderService graphLoaderService;
	/**
	 * @param args
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	public static void main(String[] args) throws Exception
	{
		String testFile="config/graphBeans.xml";
		if(args.length>0)
		{
			testFile=args[0];
		}
        GenericXmlApplicationContext context = new GenericXmlApplicationContext();
        context.load(new FileSystemResource(testFile));
        TestBatchGraphLoad testRunner=(TestBatchGraphLoad) context.getBean("testRunner");
        testRunner.runTest();        
	}
		
	public void runTest() throws Exception
	{
		PrintWriter benchmarkResultsLog=new PrintWriter(new FileOutputStream(benchmarkResultsFilename),true);
		
		FileInputStream fis = new FileInputStream(inputTestDataFile);		
		//See http://forums.sun.com/thread.jspa?threadID=5234824
		fis.read();
		fis.read();
		CBZip2InputStream zis=new CBZip2InputStream(new BufferedInputStream(fis));
		BufferedReader r=new BufferedReader(new InputStreamReader(zis));		
		
			
		//Example line from Wikipedia file:
		//	<http://dbpedia.org/resource/Calf> <http://dbpedia.org/ontology/wikiPageWikiLink> <http://dbpedia.org/resource/Veal> .	
		Pattern p=Pattern.compile("<http://dbpedia.org/resource/([^>]*)> <[^>]*> <http://dbpedia.org/resource/([^>]*)> .");
		
		String line=r.readLine();
		int numRecordsProcessed=0;
		long start=System.currentTimeMillis();
				
		benchmarkResultsLog.println("NumRecords,timeForLast"+batchReportTime+"Records(ms)");
		while(line!=null)
		{
			Matcher m=p.matcher(line);
			if(m.matches())
			{
				numRecordsProcessed++;
				if(numRecordsToLoad==numRecordsProcessed)
				{
					break;
				}
				String from=m.group(1);
				String to=m.group(2);
				if(!from.equals(to))
				{
					graphLoaderService.addLink(from, to);
				}
				if(numRecordsProcessed%batchReportTime==0)
				{
					long diff=System.currentTimeMillis()-start;
					System.out.println("Processed "+numRecordsProcessed+" records." +
							"Last batch in " +diff+" ms,"+
							" last record links ["+from+"] to ["+to+"]");
					benchmarkResultsLog.println(numRecordsProcessed+","+diff);
					if(stopFileName!=null)
					{
						File stopFile=new File(stopFileName);
						if(stopFile.exists())
						{
							stopFile.delete();
							break;					
						}
					}
					start=System.currentTimeMillis();
				}
			}			
			line=r.readLine();
		}
		System.out.println("Issuing close request");
		graphLoaderService.close();
		long diff=System.currentTimeMillis()-start;
		benchmarkResultsLog.println(numRecordsProcessed+","+diff);
		
		benchmarkResultsLog.close();
		fis.close();
	}

	public String getBenchmarkResultsFilename()
	{
		return benchmarkResultsFilename;
	}

	public void setBenchmarkResultsFilename(String benchmarkResultsFilename)
	{
		this.benchmarkResultsFilename = benchmarkResultsFilename;
	}

	public String getInputTestDataFile()
	{
		return inputTestDataFile;
	}

	public void setInputTestDataFile(String inputTestDataFile)
	{
		this.inputTestDataFile = inputTestDataFile;
	}

	public int getBatchReportTime()
	{
		return batchReportTime;
	}

	public void setBatchReportTime(int batchReportTime)
	{
		this.batchReportTime = batchReportTime;
	}

	public String getStopFileName()
	{
		return stopFileName;
	}

	public void setStopFileName(String stopFileName)
	{
		this.stopFileName = stopFileName;
	}

	public GraphLoaderService getGraphLoaderService()
	{
		return graphLoaderService;
	}

	public void setGraphLoaderService(GraphLoaderService graphLoaderService)
	{
		this.graphLoaderService = graphLoaderService;
	}

	public int getNumRecordsToLoad()
	{
		return numRecordsToLoad;
	}

	public void setNumRecordsToLoad(int numRecordsToLoad)
	{
		this.numRecordsToLoad = numRecordsToLoad;
	}

}
