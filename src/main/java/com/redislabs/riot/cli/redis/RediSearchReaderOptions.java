package com.redislabs.riot.cli.redis;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.search.Limit;
import com.redislabs.lettusearch.search.SearchOptions;
import com.redislabs.lettusearch.search.SearchOptions.SearchOptionsBuilder;
import com.redislabs.riot.redisearch.RediSearchReader;

import picocli.CommandLine.Option;

public class RediSearchReaderOptions {

	@Option(names = "--source-index", description = "RediSearch index", paramLabel = "<name>")
	private String index;
	@Option(names = "--query", description = "RediSearch query (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
	private String query = "*";
	@Option(names = "--num", description = "Limit results to number (default: ${DEFAULT-VALUE})", paramLabel = "<count>")
	private long limitNum = Limit.DEFAULT_NUM;
	@Option(names = "--offset", description = "Limit results to offset (default: ${DEFAULT-VALUE})", paramLabel = "<count>")
	private long limitOffset = Limit.DEFAULT_OFFSET;

	public RediSearchReader reader(RediSearchClient client) {
		SearchOptionsBuilder builder = SearchOptions.builder();
		builder.limit(Limit.builder().num(limitNum).offset(limitOffset).build());
		return new RediSearchReader(client, index, query, builder.build());
	}

	public boolean isSet() {
		return index != null;
	}

}
