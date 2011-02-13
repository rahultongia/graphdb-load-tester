package com.tinkerpop.graph.benchmark.loader;

import com.tinkerpop.graph.benchmark.index.SimpleKeyToNodeIdIndex;

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


/**
 * Implementation which builds a Vertex index only in order to benchmark
 * the raw cost of different choices of index services. No Graph database
 * is used to store the Vertices or edges - a simple simulator allocates
 * a unique long number to represent the Vertex ID that the missing graph
 * database would have allocated.
 */
public class GraphIndexOnlyLoaderServiceImpl implements GraphLoaderService
{

	private SimpleKeyToNodeIdIndex keyService;
	long nextId=0;
	@Override
	public void addLink(String fromNodeKey, String toNodeKey)
	{
		possiblyAddKey(fromNodeKey);
		possiblyAddKey(toNodeKey);
	}

	private void possiblyAddKey(String key)
	{
		if(getKeyService().getGraphNodeId(key)<0)
		{
			getKeyService().put(key, nextId++);
		}
	}

	@Override
	public void close()
	{
		getKeyService().close();
	}

	public void setKeyService(SimpleKeyToNodeIdIndex keyService)
	{
		this.keyService = keyService;
	}

	public SimpleKeyToNodeIdIndex getKeyService()
	{
		return keyService;
	}

}
