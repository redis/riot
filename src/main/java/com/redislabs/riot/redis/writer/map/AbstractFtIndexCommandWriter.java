package com.redislabs.riot.redis.writer.map;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.text.StrSubstitutor;

import com.redislabs.riot.redis.RedisCommands;
import com.redislabs.riot.redis.writer.RediSearchCommandWriter;

import lombok.Setter;
import lombok.Singular;

@SuppressWarnings("rawtypes")
public abstract class AbstractFtIndexCommandWriter extends AbstractMapCommandWriter
		implements RediSearchCommandWriter<Map<String, Object>> {

	private @Setter String index;
	private @Setter String query;
	@Singular
	private @Setter List<String> options;

	protected AbstractFtIndexCommandWriter(String index, String query, List<String> options) {
		this.index = index;
		this.query = query;
		this.options = options;
	}

	@Override
	protected Object write(RedisCommands commands, Object redis, Map<String, Object> item) {
		StrSubstitutor substitutor = new StrSubstitutor(item);
		Object[] options = new String[this.options.size()];
		for (int index = 0; index < options.length; index++) {
			options[index] = substitutor.replace(this.options.get(index));
		}
		return write(commands, redis, index, substitutor.replace(query), options);
	}

	protected abstract Object write(RedisCommands commands, Object redis, String index, String query,
			Object... options);

}
