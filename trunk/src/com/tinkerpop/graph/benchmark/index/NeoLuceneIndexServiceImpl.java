package com.tinkerpop.graph.benchmark.index;
/**
 *   By virtue of the Neo4j dependency..... 
 * 
 *   This program is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU Affero General Public License as
 *    published by the Free Software Foundation, either version 3 of the
 *    License, or (at your option) any later version.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
import org.neo4j.index.lucene.LuceneIndexBatchInserterImpl;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;


/**
 * A wrapper for the Neo4j Batch index service
 * @author MAHarwood
 *
 */
public class NeoLuceneIndexServiceImpl implements SimpleKeyToNodeIdIndex
{

	private LuceneIndexBatchInserterImpl indexService;

	public NeoLuceneIndexServiceImpl(BatchInserter inserter)
	{
		// create the batch index service
		indexService = new LuceneIndexBatchInserterImpl( inserter );
	}

	@Override
	public void close()
	{
		indexService.optimize();
		indexService.shutdown();		
	}

	@Override
	public long getGraphNodeId(String udk)
	{
		return indexService.getSingleNode("udk", udk); 
	}

	@Override
	public void put(String udk, long graphNodeId)
	{
		indexService.index(graphNodeId, "udk", udk); 	
	}

}
