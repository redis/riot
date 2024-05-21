package com.redis.riot.core.function;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import org.springframework.batch.item.ItemProcessor;

import com.redis.lettucemod.timeseries.Sample;
import com.redis.spring.batch.common.DataType;
import com.redis.spring.batch.common.KeyValue;

import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;

public class KeyValueMapProcessor implements ItemProcessor<KeyValue<String, Object>, Map<String, Object>> {

	private Function<String, Map<String, String>> key = t -> Collections.emptyMap();
	private UnaryOperator<Map<String, String>> hash = UnaryOperator.identity();
	private Function<List<Sample>, Map<String, String>> timeseries = this::timeseriesToMap;
	private StreamToMapFunction stream = new StreamToMapFunction();
	private CollectionToMapFunction list = new CollectionToMapFunction();
	private CollectionToMapFunction set = new CollectionToMapFunction();
	private ZsetToMapFunction zset = new ZsetToMapFunction();
	private Function<String, Map<String, String>> json = new StringToMapFunction();
	private Function<String, Map<String, String>> string = new StringToMapFunction();
	private Function<Object, Map<String, String>> defaultFunction = s -> Collections.emptyMap();

	@Override
	public Map<String, Object> process(KeyValue<String, Object> item) {
		Map<String, Object> map = new LinkedHashMap<>();
		map.putAll(key.apply(item.getKey()));
		map.putAll(value(item));
		return map;
	}

	@SuppressWarnings("unchecked")
	private Map<String, String> value(KeyValue<String, Object> item) {
		if (!KeyValue.hasType(item) || !KeyValue.hasValue(item)) {
			return Collections.emptyMap();
		}
		DataType type = KeyValue.type(item);
		if (type == null) {
			return Collections.emptyMap();
		}
		switch (type) {
		case HASH:
			return hash.apply((Map<String, String>) item.getValue());
		case LIST:
			return list.apply((List<String>) item.getValue());
		case SET:
			return set.apply((Collection<String>) item.getValue());
		case ZSET:
			return zset.apply((List<ScoredValue<String>>) item.getValue());
		case STREAM:
			return stream.apply((List<StreamMessage<String, String>>) item.getValue());
		case JSON:
			return json.apply((String) item.getValue());
		case STRING:
			return string.apply((String) item.getValue());
		case TIMESERIES:
			return timeseries.apply((List<Sample>) item.getValue());
		default:
			return defaultFunction.apply(item.getValue());
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

}
