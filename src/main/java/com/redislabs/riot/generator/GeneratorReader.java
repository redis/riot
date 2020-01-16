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
import org.springframework.expression.spel.support.SimpleEvaluationContext.Builder;
import org.springframework.lang.Nullable;
import org.springframework.util.ClassUtils;

import com.redislabs.riot.transfer.FlowThread;

import lombok.Setter;

public class GeneratorReader extends AbstractItemStreamItemReader<Map<String, Object>> {

	public final static String FIELD_INDEX = "index";
	public final static String FIELD_PARTITION = "partition";
	public final static String FIELD_PARTITIONS = "partitions";

	@Setter
	private Locale locale;
	private int maxItemCount = Integer.MAX_VALUE;
	private Map<String, Expression> fakerFields = new HashMap<>();
	private Map<String, String> simpleFields = new LinkedHashMap<>();

	private ThreadLocal<Integer> partition = new ThreadLocal<>();
	private ThreadLocal<Integer> partitions = new ThreadLocal<>();
	private ThreadLocal<EvaluationContext> context = new ThreadLocal<>();
	private ThreadLocal<Integer> offset = new ThreadLocal<>();
	private ThreadLocal<Integer> currentItemCount = new ThreadLocal<>();
	private ThreadLocal<Integer> partitionSize = new ThreadLocal<>();

	public GeneratorReader() {
		setName(ClassUtils.getShortName(getClass()));
	}

	public int partition() {
		return partition.get();
	}

	public int partitions() {
		return partitions.get();
	}

	public GeneratorReader setFakerFields(Map<String, String> fields) {
		SpelExpressionParser parser = new SpelExpressionParser();
		fields.forEach((k, v) -> fakerFields.put(k, parser.parseExpression(v)));
		return this;
	}

	public GeneratorReader setSimpleFields(Map<String, Integer> fields) {
		for (Entry<String, Integer> field : fields.entrySet()) {
			String string = StringUtils.leftPad("", field.getValue(), "x");
			simpleFields.put(field.getKey(), string);
		}
		return this;
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
		Builder builder = new Builder(accessor).withInstanceMethods().withRootObject(faker);
		this.context.set(builder.build());
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
		map.put(FIELD_INDEX, index());
		map.put(FIELD_PARTITION, partition.get());
		map.put(FIELD_PARTITIONS, partitions.get());
		for (Entry<String, Expression> entry : fakerFields.entrySet()) {
			map.put(entry.getKey(), entry.getValue().getValue(context.get()));
		}
		for (Entry<String, String> entry : simpleFields.entrySet()) {
			map.put(entry.getKey(), entry.getValue());
		}
		return map;
	}

}
