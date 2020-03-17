package com.redislabs.riot.redis.writer.keyvalue;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.redislabs.riot.redis.RedisCommands;
import com.redislabs.riot.redis.reader.KeyValue;
import com.redislabs.riot.redis.writer.AbstractCommandWriter;

import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;

public class KeyValueCommandWriter extends AbstractCommandWriter<KeyValue> {

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected Object write(RedisCommands commands, Object redis, KeyValue item) throws Exception {
		String key = item.getKey();
		Object value = item.getValue();
		switch (item.getType()) {
		case HASH:
			return commands.hmset(redis, key, (Map) value);
		case LIST:
			return commands.lpush(redis, key,
					((List<String>) value).toArray(new String[((List<String>) value).size()]));
		case SET:
			return commands.sadd(redis, key, ((Set<String>) value).toArray(new String[((Set<String>) value).size()]));
		case STREAM:
			List<StreamMessage<String, String>> messages = (List<StreamMessage<String, String>>) value;
			return commands.xadd(redis, key, messages.stream().map(m -> m.getId()).collect(Collectors.toList()),
					messages.stream().map(m -> m.getBody()).collect(Collectors.toList()));
		case STRING:
			return commands.set(redis, key, (String) value);
		case ZSET:
			return commands.zadd(redis, key, (List<ScoredValue<String>>) value);
		}
		return null;
	}

}
