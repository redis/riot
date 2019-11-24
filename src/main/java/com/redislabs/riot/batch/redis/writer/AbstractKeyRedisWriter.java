package com.redislabs.riot.batch.redis.writer;

import java.util.Map;

import com.redislabs.riot.batch.redis.AbstractRedisWriter;
import com.redislabs.riot.batch.redis.RedisCommands;

import lombok.experimental.Accessors;

@Accessors(fluent = true)
public abstract class AbstractKeyRedisWriter<R> extends AbstractRedisWriter<R, Map<String, Object>> {

	protected interface KeyMaker {
		String key(Map<String, Object> item);

		static KeyMaker create(String separator, String prefix, String[] fields) {
			if (prefix == null) {
				return (Map<String, Object> item) -> {
					StringBuilder builder = new StringBuilder();
					join(builder, separator, item, fields);
					return builder.toString();
				};
			}
			if (fields.length == 0) {
				return (Map<String, Object> item) -> prefix;
			}
			return (Map<String, Object> item) -> {
				StringBuilder builder = new StringBuilder();
				builder.append(prefix);
				builder.append(separator);
				join(builder, separator, item, fields);
				return builder.toString();
			};
		}

		static void join(StringBuilder builder, String separator, Map<String, Object> item, String[] fields) {
			for (int index = 0; index < fields.length; index++) {
				builder.append(String.valueOf(item.getOrDefault(fields[index], "")));
				if (index < fields.length - 1) {
					builder.append(separator);
				}
			}
		}
	}

	private KeyMaker keymaker;

	protected String keySeparator;

	public void key(String separator, String prefix, String[] fields) {
		this.keySeparator = separator;
		this.keymaker = KeyMaker.create(separator, prefix, fields);
	}

//	@Override
//	public String toString() {
//		if (keyspace == null) {
//			return keysDescription();
//		}
//		if (keys.length > 0) {
//			return keyspace + separator + keysDescription();
//		}
//		return keyspace;
//	}
//
//	private String keysDescription() {
//		return String.join(separator, wrap(keys));
//	}
//
//	private String[] wrap(String[] fields) {
//		String[] results = new String[fields.length];
//		for (int index = 0; index < fields.length; index++) {
//			results[index] = "<" + fields[index] + ">";
//		}
//		return results;
//	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected Map stringMap(Map map) {
		map.forEach((k, v) -> map.put(k, convert(v, String.class)));
		return map;
	}

	@Override
	protected Object write(RedisCommands<R> commands, R redis, Map<String, Object> item) {
		String key = keymaker.key(item);
		return write(commands, redis, key, item);
	}

	protected abstract Object write(RedisCommands<R> commands, R redis, String key, Map<String, Object> item);

}
