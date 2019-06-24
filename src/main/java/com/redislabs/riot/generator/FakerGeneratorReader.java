package com.redislabs.riot.generator;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.support.SimpleEvaluationContext.Builder;
import org.springframework.util.ClassUtils;

import com.redislabs.riot.IndexedPartitioner;

import lombok.Setter;

public class FakerGeneratorReader extends AbstractItemCountingItemStreamItemReader<Map<String, Object>> {

	private ThreadLocal<Long> current = new ThreadLocal<>();
	@Setter
	private Locale locale;
	@Setter
	private Map<String, Expression> fieldExpressions;
	private ThreadLocal<EvaluationContext> context = new ThreadLocal<>();
	private ThreadLocal<Integer> partitionIndex = new ThreadLocal<>();
	private ThreadLocal<Integer> partitions = new ThreadLocal<>();
	private int maxItemCount;

	public FakerGeneratorReader() {
		setName(ClassUtils.getShortName(FakerGeneratorReader.class));
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
		ReflectivePropertyAccessor accessor = new ReflectivePropertyAccessor();
		GeneratorFaker faker = new GeneratorFaker(locale, this);
		context.set(new Builder(accessor).withInstanceMethods().withRootObject(faker)
				.build());
		current.set(0l);
	}

	public void setMaxItemCount(int count) {
		this.maxItemCount = count;
		super.setMaxItemCount(count);
	}

	public long getSequence() {
		// Start at 1
		return start(maxItemCount) + current.get() + 1;
	}

	private long start(long total) {
		return total * partitionIndex.get() / partitions.get();
	}

	@Override
	protected Map<String, Object> doRead() throws Exception {
		Map<String, Object> map = new HashMap<>();
		fieldExpressions.forEach((k, v) -> map.put(k, v.getValue(context.get())));
		current.set(current.get() + 1);
		return map;
	}

	@Override
	protected void doClose() throws Exception {
		context.remove();
	}

}
