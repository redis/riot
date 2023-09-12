package com.redis.riot.core.operation;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.function.ToLongFunction;

import com.redis.riot.core.function.FieldExtractorFactory;
import com.redis.riot.core.function.IdFunctionBuilder;
import com.redis.spring.batch.writer.Operation;
import com.redis.spring.batch.writer.operation.AbstractOperation;

public abstract class AbstractMapOperationBuilder<B extends AbstractMapOperationBuilder<B>> {

    public static final String DEFAULT_SEPARATOR = IdFunctionBuilder.DEFAULT_SEPARATOR;

    public static final boolean DEFAULT_REMOVE_FIELDS = false;

    public static final boolean DEFAULT_IGNORE_MISSING_FIELDS = false;

    private String keySeparator = DEFAULT_SEPARATOR;

    private String keyspace;

    private List<String> keys;

    private boolean removeFields = DEFAULT_REMOVE_FIELDS;

    private boolean ignoreMissingFields = DEFAULT_IGNORE_MISSING_FIELDS;

    @SuppressWarnings("unchecked")
    public B keyspace(String keyspace) {
        this.keyspace = keyspace;
        return (B) this;
    }

    public B keys(String... keys) {
        return keys(Arrays.asList(keys));
    }

    @SuppressWarnings("unchecked")
    public B keys(List<String> keys) {
        this.keys = keys;
        return (B) this;
    }

    @SuppressWarnings("unchecked")
    public B keySeparator(String separator) {
        this.keySeparator = separator;
        return (B) this;
    }

    @SuppressWarnings("unchecked")
    public B removeFields(boolean remove) {
        this.removeFields = remove;
        return (B) this;
    }

    @SuppressWarnings("unchecked")
    public B ignoreMissingFields(boolean ignore) {
        this.ignoreMissingFields = ignore;
        return (B) this;
    }

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
        return new IdFunctionBuilder().separator(keySeparator).remove(removeFields).prefix(prefix).fields(fields).build();
    }

    public Operation<String, String, Map<String, Object>> build() {
        AbstractOperation<String, String, Map<String, Object>> operation = operation();
        operation.setKey(idFunction(keyspace, keys));
        return operation;
    }

    protected abstract AbstractOperation<String, String, Map<String, Object>> operation();

}
