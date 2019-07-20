package com.redislabs.riot.cli;

import java.util.Map;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;

import com.redislabs.lettusearch.search.Limit;
import com.redislabs.lettusearch.search.SearchOptions;
import com.redislabs.lettusearch.search.SearchOptions.SearchOptionsBuilder;
import com.redislabs.riot.redisearch.RediSearchReader;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "redisearch", description = "RediSearch database")
public class RediSearchReaderCommand extends AbstractReaderCommand {

	@ArgGroup(exclusive = false, heading = "Redis connection%n")
	private RedisConnectionOptions redis = new RedisConnectionOptions();
	@Parameters(paramLabel = "INDEX", description = "Name of the RediSearch index")
	private String index;
	@Option(names = "--query", description = "RediSearch query", paramLabel = "<string>")
	private String query = "*";
	@Option(names = "--num", description = "Limit results to number", paramLabel = "<count>")
	private long limitNum = Limit.DEFAULT_NUM;
	@Option(names = "--offset", description = "Limit results to offset", paramLabel = "<count>")
	private long limitOffset = Limit.DEFAULT_OFFSET;

	@Override
	protected AbstractItemCountingItemStreamItemReader<Map<String, Object>> reader() throws Exception {
		SearchOptionsBuilder options = SearchOptions.builder();
		options.limit(Limit.builder().num(limitNum).offset(limitOffset).build());
		return new RediSearchReader(redis.lettusearchPool(), index, query, options.build());
	}

	@Override
	protected String description() {
		return "RediSearch index '" + index + "' '" + query + "'";
	}

}
