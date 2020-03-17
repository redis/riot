package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import com.redislabs.riot.redis.RedisCommands;

import io.lettuce.core.ScriptOutputType;
import lombok.Setter;
import lombok.experimental.Accessors;

@SuppressWarnings({ "rawtypes", "unchecked" })
@Accessors(fluent = true)
public class Evalsha extends AbstractMapCommandWriter {

	@Setter
	private String sha;
	@Setter
	private String[] keys;
	@Setter
	private String[] args;
	@Setter
	private ScriptOutputType outputType;

	@Override
	protected Object write(RedisCommands commands, Object redis, Map<String, Object> item) {
		String[] keys = array(this.keys, item);
		String[] args = array(this.args, item);
		return commands.evalsha(redis, sha, outputType, keys, args);
	}

	private String[] array(String[] fields, Map<String, Object> item) {
		String[] array = new String[fields.length];
		for (int index = 0; index < fields.length; index++) {
			array[index] = convert(item.get(fields[index]), String.class);
		}
		return array;
	}

}