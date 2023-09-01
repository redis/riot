package com.redis.riot.core.function;

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

public class KeyValueToMapFunction implements Function<KeyValue<String>, Map<String, Object>> {

    private final Function<String, Map<String, String>> keyFieldsExtractor;

    private UnaryOperator<Map<String, String>> hash = UnaryOperator.identity();

    private SampleToMapFunction timeseries = new SampleToMapFunction();

    private StreamToMapFunction stream = new StreamToMapFunction();

    private CollectionToStringMapFunction list = new CollectionToStringMapFunction();

    private CollectionToStringMapFunction set = new CollectionToStringMapFunction();

    private ZsetToMapFunction zset = new ZsetToMapFunction();

    private Function<String, Map<String, String>> json = new StringToMapFunction();

    private Function<String, Map<String, String>> string = new StringToMapFunction();

    private Function<Object, Map<String, String>> defaultFunction = s -> null;

    public KeyValueToMapFunction(Function<String, Map<String, String>> keyFieldsExtractor) {
        this.keyFieldsExtractor = keyFieldsExtractor;
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
    public Map<String, Object> apply(KeyValue<String> item) {
        Map<String, Object> map = new LinkedHashMap<>();
        if (item.getKey() != null) {
            Map<String, String> keyFields = keyFieldsExtractor.apply(item.getKey());
            if (keyFields != null) {
                map.putAll(keyFields);
            }
        }
        if (item.getType() != null && item.getValue() != null) {
            Map<String, String> valueMap = valueMap(item.getType(), item.getValue());
            if (valueMap != null) {
                map.putAll(valueMap);
            }
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> valueMap(String type, Object value) {
        switch (type) {
            case KeyValue.HASH:
                return hash.apply((Map<String, String>) value);
            case KeyValue.LIST:
                return list.apply((List<String>) value);
            case KeyValue.SET:
                return set.apply((Set<String>) value);
            case KeyValue.ZSET:
                return zset.apply((List<ScoredValue<String>>) value);
            case KeyValue.STREAM:
                return stream.apply((List<StreamMessage<String, String>>) value);
            case KeyValue.JSON:
                return json.apply((String) value);
            case KeyValue.STRING:
                return string.apply((String) value);
            case KeyValue.TIMESERIES:
                return timeseries.apply((List<Sample>) value);
            default:
                return defaultFunction.apply(value);
        }
    }

}
