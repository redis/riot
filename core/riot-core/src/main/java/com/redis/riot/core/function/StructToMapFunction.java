package com.redis.riot.core.function;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import com.redis.lettucemod.timeseries.Sample;
import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.KeyValue.Type;

import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;

public class StructToMapFunction implements Function<KeyValue<String, Object>, Map<String, Object>> {

	private Function<String, Map<String, String>> key = t -> Collections.emptyMap();
	private UnaryOperator<Map<String, String>> hash = UnaryOperator.identity();
	private Function<List<Sample>, Map<String, String>> timeseries = this::timeseriesToMap;
	private StreamToMapFunction stream = new StreamToMapFunction();
	private CollectionToMapFunction list = new CollectionToMapFunction();
	private CollectionToMapFunction set = new CollectionToMapFunction();
	private ZsetToMapFunction zset = new ZsetToMapFunction();
	private Function<String, Map<String, String>> json = new StringToMapFunction();
	private Function<String, Map<String, String>> string = new StringToMapFunction();
	private Function<Object, Map<String, String>> defaultFunction = s -> null;

	public void setKey(Function<String, Map<String, String>> key) {
		this.key = key;
	}

	public void setHash(UnaryOperator<Map<String, String>> function) {
		this.hash = function;
	}

	public void setStream(StreamToMapFunction function) {
		this.stream = function;
	}

	public void setList(CollectionToMapFunction function) {
		this.list = function;
	}

	public void setSet(CollectionToMapFunction function) {
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
	public Map<String, Object> apply(KeyValue<String, Object> t) {
		Map<String, Object> map = new LinkedHashMap<>();
		map.putAll(key.apply(t.getKey()));
		map.putAll(value(t.getType(), t.getValue()));
		return map;
	}

	@SuppressWarnings("unchecked")
	private Map<String, String> value(Type type, Object value) {
		if (type == null || value == null) {
			return Collections.emptyMap();
		}
		switch (type) {
		case HASH:
			return hash.apply((Map<String, String>) value);
		case LIST:
			return list.apply((List<String>) value);
		case SET:
			return set.apply((Collection<String>) value);
		case ZSET:
			return zset.apply((List<ScoredValue<String>>) value);
		case STREAM:
			return stream.apply((List<StreamMessage<String, String>>) value);
		case JSON:
			return json.apply((String) value);
		case STRING:
			return string.apply((String) value);
		case TIMESERIES:
			return timeseries.apply((List<Sample>) value);
		default:
			return defaultFunction.apply(value);
		}
	}

	private Map<String, String> timeseriesToMap(List<Sample> source) {
		Map<String, String> result = new HashMap<>();
		for (int index = 0; index < source.size(); index++) {
			Sample sample = source.get(index);
			result.put(String.valueOf(index), sample.getTimestamp() + ":" + sample.getValue());
		}
		return result;
	}

}
