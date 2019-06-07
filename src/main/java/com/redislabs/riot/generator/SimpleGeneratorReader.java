package com.redislabs.riot.generator;

import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

import com.redislabs.riot.batch.IndexedPartitioner;

public class SimpleGeneratorReader extends AbstractItemCountingItemStreamItemReader<Map<String, Object>> {

	private ThreadLocal<Long> current = new ThreadLocal<>();
	private ThreadLocal<Integer> partitionIndex = new ThreadLocal<>();
	private ThreadLocal<Integer> partitions = new ThreadLocal<>();
	private int maxItemCount;

	public SimpleGeneratorReader() {
		setName(ClassUtils.getShortName(SimpleGeneratorReader.class));
	}

	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		this.partitionIndex.set(IndexedPartitioner.getPartitionIndex(executionContext));
		this.partitions.set(IndexedPartitioner.getPartitions(executionContext));
		super.open(executionContext);
	}

	public int getPartitions() {
		return partitions.get();
	}

	public int getPartitionIndex() {
		return partitionIndex.get();
	}

	@Override
	protected void doOpen() throws Exception {
		current.set(0l);
	}

	public void setMaxItemCount(int count) {
		this.maxItemCount = count;
		super.setMaxItemCount(count);
	}

	public long getSequence() {
		return start(maxItemCount) + current.get();
	}

	private long start(long total) {
		return total * partitionIndex.get() / partitions.get();
	}

	@Override
	protected Map<String, Object> doRead() throws Exception {
		Map<String, Object> map = new HashMap<>();
		map.put("field", current.get());
		current.set(current.get() + 1);
		return map;
	}

	@Override
	protected void doClose() throws Exception {
	}

}
