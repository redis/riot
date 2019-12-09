package com.redislabs.riot.batch.generator;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.support.SimpleEvaluationContext.Builder;
import org.springframework.lang.Nullable;
import org.springframework.util.ClassUtils;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Accessors(fluent = true)
public class GeneratorReader extends AbstractItemCountingItemStreamItemReader<Map<String, Object>> {

	public final static String FIELD_INDEX = "index";
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
	private Map<String, String> simpleFields = new LinkedHashMap<>();
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
		for (Entry<String, Integer> field : fieldSizes.entrySet()) {
			String string = StringUtils.leftPad("", field.getValue(), "x");
			simpleFields.put(field.getKey(), string);
		}
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

	@Nullable
	@Override
	public Map<String, Object> read() throws Exception, UnexpectedInputException, ParseException {
		Map<String, Object> item = super.read();
		if (item == null) {
			log.debug("Null - {} items read", getCurrentItemCount());
		}
		return item;
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
		for (Entry<String, String> entry : simpleFields.entrySet()) {
			map.put(entry.getKey(), entry.getValue());
		}
		return map;
	}

	@Override
	protected void doClose() throws Exception {
		this.context = null;
	}

}
