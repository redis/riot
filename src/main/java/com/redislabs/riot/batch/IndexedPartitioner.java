/*
 * Copyright 2006-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.redislabs.riot.batch;

import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;

public class IndexedPartitioner implements Partitioner {

	public static final String PARTITION_KEY = "partition";
	public static final String CONTEXT_KEY_INDEX = "index";
	public static final String CONTEXT_KEY_PARTITIONS = "partitions";
	private int size;

	public IndexedPartitioner(int size) {
		this.size = size;
	}

	@Override
	public Map<String, ExecutionContext> partition(int gridSize) {
		Map<String, ExecutionContext> map = new HashMap<String, ExecutionContext>(gridSize);
		for (int index = 0; index < size; index++) {
			ExecutionContext context = new ExecutionContext();
			context.putInt(CONTEXT_KEY_INDEX, index);
			context.putInt(CONTEXT_KEY_PARTITIONS, size);
			map.put(PARTITION_KEY + String.valueOf(index), context);
		}
		return map;
	}

	public static int getPartitionIndex(ExecutionContext executionContext) {
		if (executionContext.containsKey(IndexedPartitioner.CONTEXT_KEY_INDEX)) {
			return executionContext.getInt(IndexedPartitioner.CONTEXT_KEY_INDEX);
		}
		return 0;
	}

	public static int getPartitions(ExecutionContext executionContext) {
		if (executionContext.containsKey(IndexedPartitioner.CONTEXT_KEY_PARTITIONS)) {
			return executionContext.getInt(IndexedPartitioner.CONTEXT_KEY_PARTITIONS);
		}
		return 1;
	}

}