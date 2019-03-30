package com.redislabs.recharge.generator;

import java.util.Locale;
import java.util.Map;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.recharge.IndexedPartitioner;
import com.redislabs.recharge.processor.CachedRedis;

public class GeneratorReader extends AbstractItemCountingItemStreamItemReader<Map<String, Object>> {

	private volatile boolean initialized = false;
	private Object lock = new Object();
	private volatile int current = 0;
	private StatefulRediSearchConnection<String, String> connection;
	private String locale;
	private StandardEvaluationContext evaluationContext;
	private int partitionIndex;
	private int partitions;
	private MapGenerator generator;

	public GeneratorReader() {
		setName(ClassUtils.getShortName(GeneratorReader.class));
	}

	public void setGenerator(MapGenerator generator) {
		this.generator = generator;
	}

	public void setLocale(String locale) {
		this.locale = locale;
	}

	public void setConnection(StatefulRediSearchConnection<String, String> connection) {
		this.connection = connection;
	}

	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		this.partitionIndex = IndexedPartitioner.getPartitionIndex(executionContext);
		this.partitions = IndexedPartitioner.getPartitions(executionContext);
		super.open(executionContext);
	}

	public int getPartitions() {
		return partitions;
	}

	public int getPartitionIndex() {
		return partitionIndex;
	}

	public int current() {
		return current;
	}

	@Override
	protected void doOpen() throws Exception {
		Assert.state(!initialized, "Cannot open an already open GeneratorReader, call close first");
		GeneratorRootObject root = getRootObject();
		root.setReader(this);
		this.evaluationContext = new StandardEvaluationContext(root);
		if (connection != null) {
			evaluationContext.setVariable("r", connection.sync());
			evaluationContext.setVariable("c", new CachedRedis(connection.sync()));
		}
		initialized = true;
	}

	private GeneratorRootObject getRootObject() {
		if (locale == null) {
			return new GeneratorRootObject();
		}
		return new GeneratorRootObject(new Locale(locale));
	}

	public long current(long end) {
		return current / segment(end);
	}

	public long nextLong(long end) {
		return nextLong(0, end);
	}

	public long nextLong(long start, long end) {
		long segment = segment(start, end);
		long partitionStart = start + partitionIndex * segment;
		return partitionStart + (current % segment);
	}

	public long segment(long end) {
		return segment(0, end);
	}

	public long segment(long start, long end) {
		return (end - start) / partitions;
	}

	public String nextId(long start, long end, String format) {
		return String.format(format, nextLong(start, end));
	}

	public String nextId(long end, String format) {
		return nextId(0, end, format);
	}

	@Override
	protected Map<String, Object> doRead() throws Exception {
		synchronized (lock) {
			Map<String, Object> map = generator.generate(evaluationContext);
			current++;
			return map;
		}
	}

	@Override
	protected void doClose() throws Exception {
		synchronized (lock) {
			initialized = false;
			current = 0;
			evaluationContext = null;
		}
	}

}
