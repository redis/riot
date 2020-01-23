package com.redislabs.riot.redis.writer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import lombok.Builder;
import lombok.Data;

@Builder
public @Data class KeyBuilder {

	public final static String DEFAULT_KEY_SEPARATOR = ":";
	public final static String DEFAULT_PREFIX = "";

	private String separator;
	private String prefix;
	private List<String> fields;

	public static class KeyBuilderBuilder {

		public KeyBuilderBuilder separator(String separator) {
			this.separator = separator;
			update();
			return this;
		}

		public KeyBuilderBuilder prefix(String prefix) {
			this.prefix = prefix;
			update();
			return this;
		}

		public KeyBuilderBuilder field(String field) {
			if (this.fields == null) {
				this.fields = new ArrayList<>();
			}
			this.fields.add(field);
			update();
			return this;
		}

		public KeyBuilderBuilder fields(String... fields) {
			if (this.fields == null) {
				this.fields = new ArrayList<>();
			}
			this.fields.addAll(Arrays.asList(fields));
			update();
			return this;
		}

		private void update() {
			if (prefix == null) {
				prefix = DEFAULT_PREFIX;
			}
			if (separator == null) {
				separator = DEFAULT_KEY_SEPARATOR;
			}
			if (!prefix.isEmpty() && fields != null && !fields.isEmpty()) {
				prefix = prefix + separator;
			}
		}

	}

	public String key(Map<String, Object> item) {
		StringBuilder builder = new StringBuilder();
		builder.append(prefix);
		if (fields != null) {
			for (int index = 0; index < fields.size(); index++) {
				builder.append(String.valueOf(item.getOrDefault(fields.get(index), "")));
				if (index < fields.size() - 1) {
					builder.append(separator);
				}
			}
		}
		return builder.toString();
	}

}
