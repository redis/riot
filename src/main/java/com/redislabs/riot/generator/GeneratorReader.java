package com.redislabs.riot.generator;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

import com.redislabs.riot.batch.IndexedPartitioner;

public abstract class GeneratorReader extends AbstractItemCountingItemStreamItemReader<Map<String, Object>> {

	public static final String FIELD_INDEX = "index";
	public static final String FIELD_THREAD = "thread";
	public static final String FIELD_THREADS = "threads";
	private AtomicLong currentItemCount = new AtomicLong(0);
	private ThreadLocal<Integer> partition = new ThreadLocal<>();
	private ThreadLocal<Integer> partitions = new ThreadLocal<>();
	private int maxItemCount;

	public GeneratorReader() {
		setName(ClassUtils.getShortName(getClass()));
	}

	@Override
	protected void doOpen() throws Exception {
		// do nothing
	}

	public int partitions() {
		return partitions.get();
	}

	public int partition() {
		return partition.get();
	}

	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		this.partition.set(IndexedPartitioner.getPartitionIndex(executionContext));
		this.partitions.set(IndexedPartitioner.getPartitions(executionContext));
		super.open(executionContext);
	}

	public void setMaxItemCount(int count) {
		this.maxItemCount = count;
		super.setMaxItemCount(count);
	}

	public long index() {
		return start(maxItemCount) + currentItemCount.get();
	}

	private long start(long total) {
		return total * partition.get() / partitions.get();
	}

	@Override
	public Map<String, Object> read() throws Exception, UnexpectedInputException, ParseException {
		if (currentItemCount.incrementAndGet() > maxItemCount) {
			return null;
		}
		return super.read();
	}

	@Override
	protected Map<String, Object> doRead() throws Exception {
		Map<String, Object> map = new HashMap<>();
		map.put(FIELD_INDEX, index());
		map.put(FIELD_THREAD, partition.get());
		map.put(FIELD_THREADS, partitions.get());
		generate(map);
		return map;
	}

	protected abstract void generate(Map<String, Object> map);

	@Override
	protected void doClose() throws Exception {
	}

}
