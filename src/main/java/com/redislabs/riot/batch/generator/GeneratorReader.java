package com.redislabs.riot.batch.generator;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.support.SimpleEvaluationContext.Builder;
import org.springframework.util.ClassUtils;

import com.redislabs.riot.batch.IndexedPartitioner;

import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(fluent = true)
public class GeneratorReader extends AbstractItemCountingItemStreamItemReader<Map<String, Object>> {

	private final static String FIELD_INDEX = "index";
	public final static String FIELD_PARTITION = "partition";
	public final static String FIELD_PARTITIONS = "partitions";

	private Object lock = new Object();
	private AtomicInteger activeThreads = new AtomicInteger(0);
	private ThreadLocal<Integer> partition = new ThreadLocal<>();
	private int partitions;
	private int partitionSize;
	@Setter
	private Locale locale;
	@Setter
	private Map<String, Expression> fieldExpressions = new HashMap<>();
	@Setter
	private Map<String, Integer> fieldSizes = new HashMap<>();
	private ThreadLocal<EvaluationContext> context = new ThreadLocal<>();
	private ThreadLocal<Integer> index = new ThreadLocal<>();
	private int maxItemCount;

	public GeneratorReader() {
		setName(ClassUtils.getShortName(getClass()));
	}

	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		synchronized (lock) {
			if (executionContext.containsKey(IndexedPartitioner.CONTEXT_KEY_PARTITION)) {
				this.partition.set(executionContext.getInt(IndexedPartitioner.CONTEXT_KEY_PARTITION));
			} else {
				this.partition.set(0);
			}
			if (executionContext.containsKey(IndexedPartitioner.CONTEXT_KEY_PARTITIONS)) {
				partitions = executionContext.getInt(IndexedPartitioner.CONTEXT_KEY_PARTITIONS);
			} else {
				this.partitions = 1;
			}
			this.partitionSize = maxItemCount / partitions;
			this.index.set(partition.get() * partitionSize);
			super.open(executionContext);
		}
	}

	@Override
	protected void doOpen() throws Exception {
		GeneratorFaker faker = new GeneratorFaker(locale, this);
		ReflectivePropertyAccessor accessor = new ReflectivePropertyAccessor();
		Builder builder = new Builder(accessor).withInstanceMethods().withRootObject(faker);
		this.context.set(builder.build());
	}

	@Override
	public void setMaxItemCount(int count) {
		this.maxItemCount = count;
		super.setMaxItemCount(count);
	}

	public long index() {
		return index.get();
	}

	public int partition() {
		return partition.get();
	}

	public int partitions() {
		return partitions;
	}

	@Override
	protected Map<String, Object> doRead() throws Exception {
		Map<String, Object> map = new HashMap<>();
		map.put(FIELD_INDEX, index.get());
		map.put(FIELD_PARTITION, partition());
		map.put(FIELD_PARTITIONS, partitions());
		for (Entry<String, Expression> entry : fieldExpressions.entrySet()) {
			map.put(entry.getKey(), entry.getValue().getValue(context.get()));
		}
		for (Entry<String, Integer> entry : fieldSizes.entrySet()) {
			map.put(entry.getKey(), new String(new byte[entry.getValue()], StandardCharsets.UTF_8));
		}
		index.set(index.get() + 1);
		return map;
	}

	@Override
	protected void doClose() throws Exception {
		synchronized (lock) {
			if (activeThreads.decrementAndGet() == 0) {
				this.context = null;
			}
		}
	}

}
