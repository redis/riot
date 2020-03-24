package com.redislabs.riot.generator;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.SimpleEvaluationContext;
import org.springframework.lang.Nullable;
import org.springframework.util.ClassUtils;

import com.redislabs.riot.transfer.FlowThread;

import lombok.Builder;
import lombok.Setter;

public class GeneratorReader extends AbstractItemStreamItemReader<Map<String, Object>> {

	public final static String FIELD_INDEX = "index";
	public final static String FIELD_PARTITION = "partition";
	public final static String FIELD_PARTITIONS = "partitions";

	private @Setter int maxItemCount = Integer.MAX_VALUE;
	private @Setter Locale locale;
	private @Setter boolean includeMetadata;
	private @Setter Map<String, Expression> fakerFields = new HashMap<>();
	private @Setter Map<String, String> simpleFields = new LinkedHashMap<>();

	private ThreadLocal<Integer> partition = new ThreadLocal<>();
	private ThreadLocal<Integer> partitions = new ThreadLocal<>();
	private ThreadLocal<EvaluationContext> context = new ThreadLocal<>();
	private ThreadLocal<Integer> offset = new ThreadLocal<>();
	private ThreadLocal<Integer> currentItemCount = new ThreadLocal<>();
	private ThreadLocal<Integer> partitionSize = new ThreadLocal<>();

	public GeneratorReader() {
		setName(ClassUtils.getShortName(getClass()));
	}

	@Builder
	private GeneratorReader(Integer maxItemCount, Locale locale, Boolean includeMetadata,
			Map<String, String> fakerFields, Map<String, Integer> simpleFields) {
		if (maxItemCount != null) {
			this.maxItemCount = maxItemCount;
		}
		this.locale = locale;
		if (includeMetadata != null) {
			this.includeMetadata = includeMetadata;
		}
		SpelExpressionParser parser = new SpelExpressionParser();
		fakerFields.forEach((k, v) -> this.fakerFields.put(k, parser.parseExpression(v)));
		simpleFields.forEach((k, v) -> this.simpleFields.put(k, StringUtils.leftPad("", v, "x")));
	}

	public int partition() {
		return partition.get();
	}

	public int partitions() {
		return partitions.get();
	}

	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		this.partition.set(executionContext.getInt(FlowThread.CONTEXT_PARTITION));
		this.partitions.set(executionContext.getInt(FlowThread.CONTEXT_PARTITIONS));
		this.partitionSize.set(maxItemCount / partitions.get());
		if (partition.get() == partitions.get() - 1) {
			// make up for non exact divisions (e.g. 10/3=9, missing 1)
			partitionSize.set(maxItemCount + partitionSize.get() * (1 - partitions.get()));
		}
		this.offset.set(partition.get() * partitionSize.get());
		this.currentItemCount.set(0);
		GeneratorFaker faker = new GeneratorFaker(locale, this);
		ReflectivePropertyAccessor accessor = new ReflectivePropertyAccessor();
		this.context
				.set(new SimpleEvaluationContext.Builder(accessor).withInstanceMethods().withRootObject(faker).build());
	}

	public int index() {
		return offset.get() + currentItemCount.get();
	}

	@Override
	public void close() {
		this.context = new ThreadLocal<>();
	}

	@Nullable
	@Override
	public Map<String, Object> read() throws Exception, UnexpectedInputException, ParseException {
		currentItemCount.set(currentItemCount.get() + 1);
		Map<String, Object> map = new HashMap<>();
		if (includeMetadata) {
			map.put(FIELD_INDEX, index());
			map.put(FIELD_PARTITION, partition.get());
			map.put(FIELD_PARTITIONS, partitions.get());
		}
		for (Entry<String, Expression> entry : fakerFields.entrySet()) {
			map.put(entry.getKey(), entry.getValue().getValue(context.get()));
		}
		for (Entry<String, String> entry : simpleFields.entrySet()) {
			map.put(entry.getKey(), entry.getValue());
		}
		return map;
	}

}
