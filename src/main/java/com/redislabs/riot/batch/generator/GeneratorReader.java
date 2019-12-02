package com.redislabs.riot.batch.generator;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Accessors(fluent = true)
public class GeneratorReader extends AbstractItemCountingItemStreamItemReader<Map<String, Object>> {

	public static final String FIELD_INDEX = "index";
	public static final String FIELD_PARTITION = "partition";
	public static final String FIELD_PARTITIONS = "partitions";

	@Setter
	private Locale locale;
	@Setter
	private Map<String, Expression> fieldExpressions = new HashMap<>();
	@Setter
	private Map<String, Integer> fieldSizes = new HashMap<>();
	private int partitions;
	private Object lock = new Object();
	private AtomicInteger activeThreads = new AtomicInteger(0);
	private AtomicLong count = new AtomicLong();
	private ThreadLocal<Integer> partition = new ThreadLocal<>();
	private EvaluationContext context;

	public GeneratorReader() {
		setName(ClassUtils.getShortName(getClass()));
	}

	@Override
	protected void doOpen() throws Exception {
		synchronized (lock) {
			activeThreads.incrementAndGet();
			if (context != null) {
				return;
			}
			GeneratorFaker faker = new GeneratorFaker(locale, this);
			ReflectivePropertyAccessor accessor = new ReflectivePropertyAccessor();
			Builder builder = new Builder(accessor).withInstanceMethods().withRootObject(faker);
			this.context = builder.build();
		}

	}

	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		synchronized (lock) {
			int partitionIndex = IndexedPartitioner.getPartitionIndex(executionContext);
			log.debug("Setting partition={}", partitionIndex);
			this.partition.set(partitionIndex);
			this.partitions = IndexedPartitioner.getPartitions(executionContext);
			super.open(executionContext);
		}
	}

	public int partitions() {
		return partitions;
	}

	public int partition() {
		return partition.get();
	}

	public long index() {
		return count.get();
	}

	@Override
	protected Map<String, Object> doRead() throws Exception {
		synchronized (lock) {
			Map<String, Object> map = new HashMap<>();
			map.put(FIELD_INDEX, index());
			map.put(FIELD_PARTITION, partition.get());
			map.put(FIELD_PARTITIONS, partitions);
			fieldExpressions.forEach((k, v) -> map.put(k, v.getValue(context)));
			fieldSizes.forEach((name, size) -> map.put(name, new String(new byte[size], StandardCharsets.UTF_8)));
			setCurrentItemCount(Math.toIntExact(count.incrementAndGet()));
			return map;
		}
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
