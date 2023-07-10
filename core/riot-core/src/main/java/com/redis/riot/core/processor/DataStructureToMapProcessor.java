package com.redis.riot.core.processor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.core.convert.converter.Converter;

import com.redis.lettucemod.timeseries.Sample;
import com.redis.riot.core.convert.CollectionToStringMapConverter;
import com.redis.riot.core.convert.RegexNamedGroupsExtractor;
import com.redis.riot.core.convert.StreamToStringMapConverter;
import com.redis.riot.core.convert.StringToStringMapConverter;
import com.redis.riot.core.convert.TimeSeriesToStringMapConverter;
import com.redis.riot.core.convert.ZsetToStringMapConverter;
import com.redis.spring.batch.common.KeyValue;

import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;

public class DataStructureToMapProcessor implements ItemProcessor<KeyValue<String>, Map<String, Object>> {

	private final Converter<String, Map<String, String>> keyFieldsExtractor;
	private Converter<Map<String, String>, Map<String, String>> hashConverter = s -> s;
	private TimeSeriesToStringMapConverter timeseries = new TimeSeriesToStringMapConverter();
	private StreamToStringMapConverter stream = new StreamToStringMapConverter();
	private CollectionToStringMapConverter list = new CollectionToStringMapConverter();
	private CollectionToStringMapConverter set = new CollectionToStringMapConverter();
	private ZsetToStringMapConverter zset = new ZsetToStringMapConverter();
	private Converter<String, Map<String, String>> json = new StringToStringMapConverter();
	private Converter<String, Map<String, String>> string = new StringToStringMapConverter();
	private Converter<Object, Map<String, String>> defaultConverter = s -> null;

	public DataStructureToMapProcessor(Converter<String, Map<String, String>> keyFieldsExtractor) {
		this.keyFieldsExtractor = keyFieldsExtractor;
	}

	public void setHashConverter(Converter<Map<String, String>, Map<String, String>> hashConverter) {
		this.hashConverter = hashConverter;
	}

	public void setStream(StreamToStringMapConverter streamConverter) {
		this.stream = streamConverter;
	}

	public void setList(CollectionToStringMapConverter listConverter) {
		this.list = listConverter;
	}

	public void setSet(CollectionToStringMapConverter setConverter) {
		this.set = setConverter;
	}

	public void setZset(ZsetToStringMapConverter zsetConverter) {
		this.zset = zsetConverter;
	}

	public void setString(Converter<String, Map<String, String>> stringConverter) {
		this.string = stringConverter;
	}

	public void setDefaultConverter(Converter<Object, Map<String, String>> defaultConverter) {
		this.defaultConverter = defaultConverter;
	}

	@Override
	public Map<String, Object> process(KeyValue<String> item) throws Exception {
		if (item.getType() == null) {
			return null;
		}
		if (item.getKey() == null) {
			return null;
		}
		Map<String, String> stringMap = keyFieldsExtractor.convert(item.getKey());
		if (stringMap == null) {
			return null;
		}
		Map<String, Object> map = new HashMap<>(stringMap);
		Map<String, String> valueMap = map(item);
		if (valueMap != null) {
			map.putAll(valueMap);
		}
		return map;
	}

	@SuppressWarnings("unchecked")
	private Map<String, String> map(KeyValue<String> item) {
		switch (item.getType()) {
		case KeyValue.HASH:
			return hashConverter.convert((Map<String, String>) item.getValue());
		case KeyValue.LIST:
			return list.convert((List<String>) item.getValue());
		case KeyValue.SET:
			return set.convert((Set<String>) item.getValue());
		case KeyValue.ZSET:
			return zset.convert((List<ScoredValue<String>>) item.getValue());
		case KeyValue.STREAM:
			return stream.convert((List<StreamMessage<String, String>>) item.getValue());
		case KeyValue.JSON:
			return json.convert((String) item.getValue());
		case KeyValue.STRING:
			return string.convert((String) item.getValue());
		case KeyValue.TIMESERIES:
			return timeseries.convert((List<Sample>) item.getValue());
		default:
			return defaultConverter.convert(item.getValue());
		}
	}

	public static DataStructureToMapProcessor of(String keyRegex) {
		return new DataStructureToMapProcessor(RegexNamedGroupsExtractor.of(keyRegex));
	}

}
