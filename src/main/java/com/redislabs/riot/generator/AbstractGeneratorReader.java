package com.redislabs.riot.generator;

import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

import com.redislabs.riot.IndexedPartitioner;

public abstract class AbstractGeneratorReader extends AbstractItemCountingItemStreamItemReader<Map<String, Object>> {

	public static final String FIELD_INDEX = "index";
	public static final String FIELD_THREAD = "thread";
	public static final String FIELD_THREADS = "threads";
	private ThreadLocal<Long> current = new ThreadLocal<>();
	private ThreadLocal<Integer> partitionIndex = new ThreadLocal<>();
	private ThreadLocal<Integer> partitions = new ThreadLocal<>();
	private int maxItemCount;

	public AbstractGeneratorReader() {
		setName(ClassUtils.getShortName(getClass()));
	}

	public int partitions() {
		return partitions.get();
	}

	public int partition() {
		return partitionIndex.get();
	}

	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		this.partitionIndex.set(IndexedPartitioner.getPartitionIndex(executionContext));
		this.partitions.set(IndexedPartitioner.getPartitions(executionContext));
		super.open(executionContext);
	}

	@Override
	protected void doOpen() throws Exception {
		current.set(0l);
	}

	public void setMaxItemCount(int count) {
		this.maxItemCount = count;
		super.setMaxItemCount(count);
	}

	public long index() {
		return start(maxItemCount) + current.get();
	}

	private long start(long total) {
		return total * partitionIndex.get() / partitions.get();
	}

	@Override
	protected Map<String, Object> doRead() throws Exception {
		Map<String, Object> map = new HashMap<>();
		map.put(FIELD_INDEX, index());
		map.put(FIELD_THREAD, partitionIndex.get());
		map.put(FIELD_THREADS, partitions.get());
		generate(map);
		current.set(current.get() + 1);
		return map;
	}

	protected abstract void generate(Map<String, Object> map);

	@Override
	protected void doClose() throws Exception {
	}

}
