package com.redislabs.riot.generator;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.support.SimpleEvaluationContext;

import com.github.javafaker.Faker;
import com.redislabs.riot.AbstractReader;
import com.redislabs.riot.batch.IndexedPartitioner;

import lombok.Setter;

public class GeneratorReader extends AbstractReader {

	private ThreadLocal<Long> current = new ThreadLocal<>();
	@Setter
	private Locale locale;
	@Setter
	private Map<String, Expression> fieldExpressions;
	private Faker faker;
	private ThreadLocal<EvaluationContext> context = new ThreadLocal<>();
	private ThreadLocal<Integer> partitionIndex = new ThreadLocal<>();
	private ThreadLocal<Integer> partitions = new ThreadLocal<>();
	private int maxItemCount;

	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		this.faker = new Faker(locale);
		this.partitionIndex.set(IndexedPartitioner.getPartitionIndex(executionContext));
		this.partitions.set(IndexedPartitioner.getPartitions(executionContext));
		super.open(executionContext);
	}

	public int partitions() {
		return partitions.get();
	}

	public int partitionIndex() {
		return partitionIndex.get();
	}

	public Faker faker() {
		return faker;
	}

	@Override
	protected void doOpen() throws Exception {
		ReflectivePropertyAccessor accessor = new ReflectivePropertyAccessor();
		EvaluationContext evaluationContext = new SimpleEvaluationContext.Builder(accessor).withRootObject(this)
				.build();
		context.set(evaluationContext);
		current.set(0l);
	}

	public void setMaxItemCount(int count) {
		this.maxItemCount = count;
		super.setMaxItemCount(count);
	}

	public long sequence() {
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
