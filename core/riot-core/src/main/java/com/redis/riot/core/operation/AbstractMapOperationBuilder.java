package com.redis.riot.core.operation;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.function.ToLongFunction;

import com.redis.riot.core.function.FieldExtractorFactory;
import com.redis.riot.core.function.IdFunctionBuilder;
import com.redis.spring.batch.common.Operation;
import com.redis.spring.batch.writer.operation.AbstractKeyWriteOperation;

public abstract class AbstractMapOperationBuilder {

	public static final String DEFAULT_SEPARATOR = IdFunctionBuilder.DEFAULT_SEPARATOR;

	public static final boolean DEFAULT_REMOVE_FIELDS = false;

	public static final boolean DEFAULT_IGNORE_MISSING_FIELDS = false;

	private String keySeparator = DEFAULT_SEPARATOR;

	private String keyspace;

	private List<String> keyFields;

	private boolean removeFields = DEFAULT_REMOVE_FIELDS;

	private boolean ignoreMissingFields = DEFAULT_IGNORE_MISSING_FIELDS;

	protected Function<Map<String, Object>, String> toString(String field) {
		if (field == null) {
			return s -> null;
		}
		return fieldExtractorFactory().string(field);
	}

	private FieldExtractorFactory fieldExtractorFactory() {
		return FieldExtractorFactory.builder().remove(removeFields).nullCheck(!ignoreMissingFields).build();
	}

	protected ToLongFunction<Map<String, Object>> toLong(String field, long defaultValue) {
		if (field == null) {
			return m -> defaultValue;
		}
		return fieldExtractorFactory().longField(field, defaultValue);
	}

	protected ToDoubleFunction<Map<String, Object>> toDouble(String field, double defaultValue) {
		if (field == null) {
			return m -> defaultValue;
		}
		return fieldExtractorFactory().doubleField(field, defaultValue);
	}

	protected Function<Map<String, Object>, String> idFunction(String prefix, List<String> fields) {
		return new IdFunctionBuilder().separator(keySeparator).remove(removeFields).prefix(prefix).fields(fields)
				.build();
	}

	public Operation<String, String, Map<String, Object>, Object> build() {
		Function<Map<String, Object>, String> keyFunction = idFunction(keyspace, keyFields);
		return operation(keyFunction);
	}

	protected abstract AbstractKeyWriteOperation<String, String, Map<String, Object>> operation(
			Function<Map<String, Object>, String> keyFunction);

	public void setKeySeparator(String keySeparator) {
		this.keySeparator = keySeparator;
	}

	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}

	public void setKeyFields(String... keys) {
		setKeyFields(Arrays.asList(keys));
	}

	public void setKeyFields(List<String> keys) {
		this.keyFields = keys;
	}

	public void setRemoveFields(boolean removeFields) {
		this.removeFields = removeFields;
	}

	public void setIgnoreMissingFields(boolean ignoreMissingFields) {
		this.ignoreMissingFields = ignoreMissingFields;
	}

}
