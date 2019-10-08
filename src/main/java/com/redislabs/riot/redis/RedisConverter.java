
package com.redislabs.riot.redis;

import java.util.Map;
import java.util.StringJoiner;

public class RedisConverter {

	private String separator;
	private String keyspace;
	private String[] keys;

	public RedisConverter(String separator, String keyspace, String[] keys) {
		this.separator = separator;
		this.keyspace = keyspace;
		this.keys = keys;
	}

	public String id(Map<String, Object> item) {
		return join(item, keys);
	}

	public String key(Map<String, Object> item) {
		return key(id(item));
	}

	public String key(String id) {
		if (id == null) {
			return keyspace;
		}
		if (keyspace == null) {
			return id;
		}
		return keyspace + separator + id;
	}

	public String join(Map<String, Object> item, String[] fields) {
		if (fields.length == 0) {
			return null;
		}
		StringJoiner joiner = new StringJoiner(separator);
		for (String field : fields) {
			Object value = item.get(field);
			joiner.add(value == null ? "" : String.valueOf(value));
		}
		return joiner.toString();
	}

	@Override
	public String toString() {
		if (keyspace == null) {
			return keysDescription();
		}
		if (keys.length > 0) {
			return keyspace + separator + keysDescription();
		}
		return keyspace;
	}

	private String keysDescription() {
		return String.join(separator, wrap(keys));
	}

	private String[] wrap(String[] fields) {
		String[] results = new String[fields.length];
		for (int index = 0; index < fields.length; index++) {
			results[index] = "<" + fields[index] + ">";
		}
		return results;
	}

}
