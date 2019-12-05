package com.redislabs.riot.batch.generator;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.support.SimpleEvaluationContext.Builder;
import org.springframework.util.ClassUtils;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(fluent = true)
public class GeneratorReader extends AbstractItemCountingItemStreamItemReader<Map<String, Object>> {

	private final static String FIELD_INDEX = "index";
	public final static String FIELD_PARTITION = "partition";
	public final static String FIELD_PARTITIONS = "partitions";

	@Setter
	@Getter
	private int partition;
	@Setter
	@Getter
	private int partitions = 1;
	@Setter
	private Locale locale;
	@Setter
	private Map<String, Expression> fieldExpressions = new HashMap<>();
	@Setter
	private Map<String, Integer> fieldSizes = new HashMap<>();
	private EvaluationContext context;
	private int offset;

	public GeneratorReader() {
		setName(ClassUtils.getShortName(getClass()));
	}

	@Override
	protected void doOpen() throws Exception {
		GeneratorFaker faker = new GeneratorFaker(locale, this);
		ReflectivePropertyAccessor accessor = new ReflectivePropertyAccessor();
		Builder builder = new Builder(accessor).withInstanceMethods().withRootObject(faker);
		this.context = builder.build();
	}

	@Override
	public void setMaxItemCount(int count) {
		int partitionSize = count / partitions;
		if (partition == partitions - 1) {
			// make up for non exact divisions (e.g. 10/3=9, missing 1)
			partitionSize += count - (partitions * partitionSize);
		}
		this.offset = partition * partitionSize;
		super.setMaxItemCount(partitionSize);
	}

	public int index() {
		return offset + getCurrentItemCount();
	}

	@Override
	protected Map<String, Object> doRead() throws Exception {
		Map<String, Object> map = new HashMap<>();
		map.put(FIELD_INDEX, index());
		map.put(FIELD_PARTITION, partition());
		map.put(FIELD_PARTITIONS, partitions());
		for (Entry<String, Expression> entry : fieldExpressions.entrySet()) {
			map.put(entry.getKey(), entry.getValue().getValue(context));
		}
		for (Entry<String, Integer> entry : fieldSizes.entrySet()) {
			map.put(entry.getKey(), new String(new byte[entry.getValue()], StandardCharsets.UTF_8));
		}
		return map;
	}

	@Override
	protected void doClose() throws Exception {
		this.context = null;
	}

}
