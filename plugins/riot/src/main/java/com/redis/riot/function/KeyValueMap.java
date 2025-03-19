package com.redis.riot.function;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import com.redis.lettucemod.timeseries.Sample;
import com.redis.riot.core.processor.CollectionToMapFunction;
import com.redis.riot.core.processor.StringToMapFunction;
import com.redis.spring.batch.item.redis.common.KeyValue;

import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;

public class KeyValueMap implements Function<KeyValue<String>, Map<String, Object>> {

	private Function<String, Map<String, String>> key = t -> Collections.emptyMap();
	private Function<Map<String, String>, Map<String, String>> hash = new HashToMapFunction();
	private Function<Collection<Sample>, Map<String, String>> timeseries = new TimeSeriesToMapFunction();
	private Function<Collection<StreamMessage<String, String>>, Map<String, String>> stream = new StreamToMapFunction();
	private Function<Collection<String>, Map<String, String>> list = new CollectionToMapFunction();
	private Function<Collection<String>, Map<String, String>> set = new CollectionToMapFunction();
	private Function<Set<ScoredValue<String>>, Map<String, String>> zset = new ZsetToMapFunction();
	private Function<String, Map<String, String>> json = new StringToMapFunction();
	private Function<String, Map<String, String>> string = new StringToMapFunction();
	private Function<Object, Map<String, String>> defaultFunction = s -> Collections.emptyMap();

	@Override
	public Map<String, Object> apply(KeyValue<String> item) {
		Map<String, Object> map = new LinkedHashMap<>();
		map.putAll(key.apply(item.getKey()));
		Map<String, String> value = value(item);
		map.putAll(value);
		return map;
	}

	@SuppressWarnings("unchecked")
	private Map<String, String> value(KeyValue<String> item) {
		if (!KeyValue.hasType(item) || !KeyValue.hasValue(item)) {
			return Collections.emptyMap();
		}
		if (item.getType() == null) {
			return Collections.emptyMap();
		}
		switch (item.getType()) {
		case KeyValue.TYPE_HASH:
			return hash.apply((Map<String, String>) item.getValue());
		case KeyValue.TYPE_LIST:
			return list.apply((Collection<String>) item.getValue());
		case KeyValue.TYPE_SET:
			return set.apply((Set<String>) item.getValue());
		case KeyValue.TYPE_ZSET:
			return zset.apply((Set<ScoredValue<String>>) item.getValue());
		case KeyValue.TYPE_STREAM:
			return stream.apply((Collection<StreamMessage<String, String>>) item.getValue());
		case KeyValue.TYPE_JSON:
			return json.apply((String) item.getValue());
		case KeyValue.TYPE_STRING:
			return string.apply((String) item.getValue());
		case KeyValue.TYPE_TIMESERIES:
			return timeseries.apply((Collection<Sample>) item.getValue());
		default:
			return defaultFunction.apply(item.getValue());
		}
	}

	public void setKey(Function<String, Map<String, String>> key) {
		this.key = key;
	}

	public void setHash(Function<Map<String, String>, Map<String, String>> function) {
		this.hash = function;
	}

	public void setStream(Function<Collection<StreamMessage<String, String>>, Map<String, String>> function) {
		this.stream = function;
	}

	public void setList(Function<Collection<String>, Map<String, String>> function) {
		this.list = function;
	}

	public void setSet(Function<Collection<String>, Map<String, String>> function) {
		this.set = function;
	}

	public void setZset(Function<Set<ScoredValue<String>>, Map<String, String>> function) {
		this.zset = function;
	}

	public void setString(Function<String, Map<String, String>> function) {
		this.string = function;
	}

	public void setDefaultFunction(Function<Object, Map<String, String>> function) {
		this.defaultFunction = function;
	}

	public void setJson(Function<String, Map<String, String>> function) {
		this.json = function;
	}

	public void setTimeseries(Function<Collection<Sample>, Map<String, String>> function) {
		this.timeseries = function;
	}

}
