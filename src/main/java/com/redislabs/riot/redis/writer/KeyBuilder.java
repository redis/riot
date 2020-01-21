package com.redislabs.riot.redis.writer;

import java.util.Map;

import lombok.Builder;
import lombok.Data;

public @Data class KeyBuilder {

	public final static String DEFAULT_KEY_SEPARATOR = ":";

	private String separator;
	private String prefix;
	private String[] fields = new String[0];

	@Builder
	private KeyBuilder(String separator, String prefix, String... fields) {
		this.separator = separator == null ? DEFAULT_KEY_SEPARATOR : separator;
		this.prefix = prefix == null ? "" : fields.length == 0 ? prefix : prefix + separator;
		this.fields = fields;
	}

	public String key(Map<String, Object> item) {
		StringBuilder builder = new StringBuilder();
		builder.append(prefix);
		for (int index = 0; index < fields.length; index++) {
			builder.append(String.valueOf(item.getOrDefault(fields[index], "")));
			if (index < fields.length - 1) {
				builder.append(separator);
			}
		}
		return builder.toString();
	}

}
