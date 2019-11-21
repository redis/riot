package com.redislabs.riot.batch.redis.map;

import java.util.List;
import java.util.Map;

import io.lettuce.core.ScriptOutputType;
import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(fluent = true)
public class EvalshaMapWriter<R> extends AbstractMapWriter<R> {

	@Setter
	private String sha;
	@Setter
	private List<String> keys;
	@Setter
	private List<String> args;
	@Setter
	private ScriptOutputType outputType;

	@Override
	protected Object write(R redis, String key, Map<String, Object> item) {
		String[] keys = array(this.keys, item);
		String[] args = array(this.args, item);
		return commands.evalsha(redis, sha, outputType, keys, args);
	}

	private String[] array(List<String> fields, Map<String, Object> item) {
		String[] array = new String[fields.size()];
		for (int index = 0; index < fields.size(); index++) {
			array[index] = convert(item.get(fields.get(index)), String.class);
		}
		return array;
	}

}