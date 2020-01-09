package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import com.redislabs.riot.redis.RedisCommands;

public abstract class AbstractCollectionMapWriter<R> extends AbstractKeyMapRedisWriter<R> {

	private KeyMaker memberIdMaker;

	public void memberFields(String[] fields) {
		this.memberIdMaker = KeyMaker.create(this.keySeparator, null, fields);
	}

	@Override
	protected Object write(RedisCommands<R> commands, R redis, String key, Map<String, Object> item) {
		String member = memberIdMaker.key(item);
		return write(commands, redis, key, member, item);
	}

	protected abstract Object write(RedisCommands<R> commands, R redis, String key, String member,
			Map<String, Object> item);

}
