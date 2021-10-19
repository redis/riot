package com.redis.riot.file;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.springframework.batch.item.ItemProcessor;

import com.redis.spring.batch.support.DataStructure;

import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;

public class DataStructureItemProcessor implements ItemProcessor<DataStructure<String>, DataStructure<String>> {

	@SuppressWarnings("unchecked")
	@Override
	public DataStructure<String> process(DataStructure<String> item) throws Exception {
		String type = item.getType().toLowerCase();
		if (type.equals(DataStructure.ZSET)) {
			Collection<Map<String, Object>> zset = (Collection<Map<String, Object>>) item.getValue();
			Collection<ScoredValue<String>> values = new ArrayList<>(zset.size());
			for (Map<String, Object> map : zset) {
				double score = ((Number) map.get("score")).doubleValue();
				String value = (String) map.get("value");
				values.add((ScoredValue<String>) ScoredValue.fromNullable(score, value));
			}
			item.setValue(values);
		} else if (type.equals(DataStructure.STREAM)) {
			Collection<Map<String, Object>> stream = (Collection<Map<String, Object>>) item.getValue();
			Collection<StreamMessage<String, String>> messages = new ArrayList<>(stream.size());
			for (Map<String, Object> message : stream) {
				messages.add(new StreamMessage<>((String) message.get("stream"), (String) message.get("id"),
						(Map<String, String>) message.get("body")));
			}
			item.setValue(messages);
		}
		return item;
	}

}
