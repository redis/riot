package com.redis.riot;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import org.springframework.batch.item.ItemProcessor;

import com.redis.lettucemod.timeseries.Sample;
import com.redis.riot.core.function.CollectionToMapFunction;
import com.redis.riot.core.function.StringToMapFunction;
import com.redis.riot.function.StreamToMapFunction;
import com.redis.riot.function.ZsetToMapFunction;
import com.redis.spring.batch.item.redis.common.DataType;
import com.redis.spring.batch.item.redis.common.KeyValue;

import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;

public class KeyValueMapProcessor implements ItemProcessor<KeyValue<String, Object>, Map<String, Object>> {

	private Function<String, Map<String, String>> key = t -> Collections.emptyMap();
	private UnaryOperator<Map<String, String>> hash = UnaryOperator.identity();
	private Function<Collection<Sample>, Map<String, String>> timeseries = this::timeseriesToMap;
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
			return list.apply((Collection<String>) item.getValue());
		case SET:
			return set.apply((Set<String>) item.getValue());
		case ZSET:
			return zset.apply((Set<ScoredValue<String>>) item.getValue());
		case STREAM:
			return stream.apply((Collection<StreamMessage<String, String>>) item.getValue());
		case JSON:
			return json.apply((String) item.getValue());
		case STRING:
			return string.apply((String) item.getValue());
		case TIMESERIES:
			return timeseries.apply((Collection<Sample>) item.getValue());
		default:
			return defaultFunction.apply(item.getValue());
		}
	}

	private Map<String, String> timeseriesToMap(Collection<Sample> source) {
		Map<String, String> result = new HashMap<>();
		int index = 0;
		for (Sample sample : source) {
			result.put(String.valueOf(index), sample.getTimestamp() + ":" + sample.getValue());
			index++;
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
