package com.redislabs.riot.generator;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.support.SimpleEvaluationContext.Builder;
import org.springframework.util.ClassUtils;

import com.redislabs.riot.batch.IndexedPartitioner;

public class GeneratorReader extends AbstractItemCountingItemStreamItemReader<Map<String, Object>> {

	public static final String FIELD_INDEX = "index";
	public static final String FIELD_PARTITION = "partition";
	public static final String FIELD_PARTITIONS = "partitions";
	private ThreadLocal<Long> count = new ThreadLocal<>();
	private ThreadLocal<Integer> partition = new ThreadLocal<>();
	private int partitions;
	private int maxItemCount;
	private int partitionSize;
	private Locale locale;
	private Map<String, Expression> fieldExpressions;
	private ThreadLocal<EvaluationContext> context = new ThreadLocal<>();
	private Map<String, Integer> fieldSizes = new LinkedHashMap<>();

	public GeneratorReader(Locale locale, Map<String, Expression> fieldExpressions, Map<String, Integer> fieldSizes) {
		setName(ClassUtils.getShortName(getClass()));
		this.locale = locale;
		this.fieldExpressions = fieldExpressions;
		this.fieldSizes = fieldSizes;
	}

	@Override
	protected void doOpen() throws Exception {
		GeneratorFaker faker = new GeneratorFaker(locale, this);
		context.set(new Builder(new ReflectivePropertyAccessor()).withInstanceMethods().withRootObject(faker).build());
		count.set(0l);
	}

	public int partitions() {
		return partitions;
	}

	public int partition() {
		return partition.get();
	}

	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		this.partition.set(IndexedPartitioner.getPartitionIndex(executionContext));
		this.partitions = IndexedPartitioner.getPartitions(executionContext);
		this.partitionSize = maxItemCount / partitions;
		super.open(executionContext);
	}

	public void setMaxItemCount(int count) {
		this.maxItemCount = count;
		super.setMaxItemCount(count);
	}

	public long index() {
		return (partitionSize * partition.get()) + count.get();
	}

	@Override
	public Map<String, Object> read() throws Exception, UnexpectedInputException, ParseException {
		if (count.get() >= partitionSize) {
			return null;
		}
		return super.read();
	}

	@Override
	protected Map<String, Object> doRead() throws Exception {
		Map<String, Object> map = new HashMap<>();
		map.put(FIELD_INDEX, index());
		map.put(FIELD_PARTITION, partition.get());
		map.put(FIELD_PARTITIONS, partitions);
		fieldExpressions.forEach((k, v) -> map.put(k, v.getValue(context.get())));
		fieldSizes.forEach((name, size) -> map.put(name, new String(new byte[size], StandardCharsets.UTF_8)));
		count.set(count.get() + 1);
		return map;
	}

	@Override
	protected void doClose() throws Exception {
		context.remove();
	}

}
