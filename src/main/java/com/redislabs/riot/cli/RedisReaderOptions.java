package com.redislabs.riot.cli;

import java.util.Map;

import org.springframework.batch.item.ItemReader;

import com.redislabs.lettusearch.search.Limit;
import com.redislabs.lettusearch.search.SearchOptions;
import com.redislabs.lettusearch.search.SearchOptions.SearchOptionsBuilder;
import com.redislabs.picocliredis.RedisOptions;
import com.redislabs.riot.batch.redis.JedisMapReader;
import com.redislabs.riot.batch.redis.RediSearchItemReader;

import lombok.Data;
import picocli.CommandLine.Option;

public @Data class RedisReaderOptions {

	@Option(names = "--count", description = "Number of elements to return for each scan call", paramLabel = "<int>")
	private Integer count;
	@Option(names = "--separator", description = "Redis key separator (default: ${DEFAULT-VALUE})", paramLabel = "<str>")
	private String separator = ":";
	@Option(names = "--keyspace", description = "Redis keyspace prefix", paramLabel = "<str>")
	private String keyspace;
	@Option(names = "--keys", arity = "1..*", description = "Key fields", paramLabel = "<names>")
	private String[] keys = new String[0];
	@Option(names = "--index", description = "Index name", paramLabel = "<name>")
	private String index;
	@Option(names = "--query", description = "Search query (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
	private String query = "*";
	@Option(names = "--num", description = "Limit results to number (default: ${DEFAULT-VALUE})", paramLabel = "<count>")
	private long limitNum = Limit.DEFAULT_NUM;
	@Option(names = "--offset", description = "Limit results to offset (default: ${DEFAULT-VALUE})", paramLabel = "<count>")
	private long limitOffset = Limit.DEFAULT_OFFSET;

	public ItemReader<Map<String, Object>> reader(RedisOptions redis) {
		if (index != null) {
			return searchReader(redis);
		}
		return redisReader(redis);
	}

	public RediSearchItemReader searchReader(RedisOptions redis) {
		SearchOptionsBuilder builder = SearchOptions.builder();
		builder.limit(Limit.builder().num(limitNum).offset(limitOffset).build());
		return new RediSearchItemReader(redis.lettuSearchClient(), index, query, builder.build());
	}

	private JedisMapReader redisReader(RedisOptions redis) {
		String scanPattern = keyspace == null ? null : (keyspace + separator + "*");
		JedisMapReader reader = new JedisMapReader(redis.jedisPool());
		reader.setCount(count);
		reader.setMatch(scanPattern);
		reader.setKeys(keys);
		reader.setKeyspace(keyspace);
		reader.setSeparator(separator);
		return reader;
	}

}
