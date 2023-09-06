package com.redis.riot.core.function;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import com.redis.lettucemod.timeseries.Sample;
import com.redis.spring.batch.KeyValue;

import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;

public class KeyValueToMapFunction implements Function<KeyValue<String>, Map<String, ?>> {

    private UnaryOperator<Map<String, String>> hash = UnaryOperator.identity();

    private SampleToMapFunction timeseries = new SampleToMapFunction();

    private StreamToMapFunction stream = new StreamToMapFunction();

    private CollectionToStringMapFunction list = new CollectionToStringMapFunction();

    private CollectionToStringMapFunction set = new CollectionToStringMapFunction();

    private ZsetToMapFunction zset = new ZsetToMapFunction();

    private Function<String, Map<String, String>> json = new StringToMapFunction();

    private Function<String, Map<String, String>> string = new StringToMapFunction();

    private Function<Object, Map<String, String>> defaultFunction = s -> null;

    private ToMapFunction<KeyValue<String>, String, String> toMapFunction = toMapFunction(t -> new LinkedHashMap<>());

    public void setKeyFields(Function<String, Map<String, String>> keyFields) {
        this.toMapFunction = toMapFunction(keyFunction().andThen(keyFields));
    }

    private Function<KeyValue<String>, String> keyFunction() {
        return KeyValue::getKey;
    }

    public void setHash(UnaryOperator<Map<String, String>> function) {
        this.hash = function;
    }

    public void setStream(StreamToMapFunction function) {
        this.stream = function;
    }

    public void setList(CollectionToStringMapFunction function) {
        this.list = function;
    }

    public void setSet(CollectionToStringMapFunction function) {
        this.set = function;
    }

    public void setZset(ZsetToMapFunction function) {
        this.zset = function;
    }

    public void setString(Function<String, Map<String, String>> function) {
        this.string = function;
    }

    public void setDefaultFunction(Function<Object, Map<String, String>> function) {
        this.defaultFunction = function;
    }

    @Override
    public Map<String, ?> apply(KeyValue<String> item) {
        return toMapFunction.apply(item);
    }

    @SuppressWarnings("unchecked")
    private ToMapFunction<KeyValue<String>, String, String> toMapFunction(
            Function<KeyValue<String>, Map<String, String>> keyFields) {
        Function<KeyValue<String>, Map<String, String>> valueFunction = this::valueMap;
        return new ToMapFunction<>(keyFields, valueFunction);
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> valueMap(KeyValue<String> keyValue) {
        if (keyValue.getType() == null || keyValue.getValue() == null) {
            return Collections.emptyMap();
        }
        switch (keyValue.getType()) {
            case KeyValue.HASH:
                return hash.apply((Map<String, String>) keyValue.getValue());
            case KeyValue.LIST:
                return list.apply((List<String>) keyValue.getValue());
            case KeyValue.SET:
                return set.apply((Set<String>) keyValue.getValue());
            case KeyValue.ZSET:
                return zset.apply((List<ScoredValue<String>>) keyValue.getValue());
            case KeyValue.STREAM:
                return stream.apply((List<StreamMessage<String, String>>) keyValue.getValue());
            case KeyValue.JSON:
                return json.apply((String) keyValue.getValue());
            case KeyValue.STRING:
                return string.apply((String) keyValue.getValue());
            case KeyValue.TIMESERIES:
                return timeseries.apply((List<Sample>) keyValue.getValue());
            default:
                return defaultFunction.apply(keyValue.getValue());
        }
    }

}
