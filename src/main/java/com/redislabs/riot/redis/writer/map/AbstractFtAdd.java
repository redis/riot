package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import com.redislabs.lettusearch.search.AddOptions;
import com.redislabs.lettusearch.search.Document;
import com.redislabs.riot.redis.RedisCommands;
import com.redislabs.riot.redis.writer.KeyBuilder;
import com.redislabs.riot.redis.writer.RediSearchCommandWriter;

import lombok.Setter;

@SuppressWarnings({ "rawtypes", "unchecked" })
public abstract class AbstractFtAdd extends AbstractKeyMapCommandWriter
		implements RediSearchCommandWriter<Map<String, Object>> {

	private @Setter String index;
	private @Setter AddOptions options;
	private @Setter String score;
	private @Setter double defaultScore = 1d;

	protected AbstractFtAdd(KeyBuilder keyBuilder, boolean keepKeyFields, String index, String score,
			double defaultScore, AddOptions options) {
		super(keyBuilder, keepKeyFields);
		this.index = index;
		this.score = score;
		this.defaultScore = defaultScore;
		this.options = options;
	}

	@Override
	protected Object write(RedisCommands commands, Object redis, String key, Map<String, Object> item) {
		Document<String, String> document = Document.<String, String>builder().id(key).score(convert(item.getOrDefault(score, defaultScore), Double.class)).payload(payload(item)).build();
		item.forEach((k, v) -> document.put(k, convert(v, String.class)));
		return commands.ftadd(redis, index, document, options);
	}

	protected abstract String payload(Map<String, Object> item);

}
