package com.redislabs.riot.cli.redis;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.search.Limit;
import com.redislabs.lettusearch.search.SearchOptions;
import com.redislabs.lettusearch.search.SearchOptions.SearchOptionsBuilder;
import com.redislabs.riot.redisearch.RediSearchReader;

import io.lettuce.core.RedisURI;
import io.lettuce.core.resource.ClientResources;
import picocli.CommandLine.Option;

public class RediSearchReaderOptions {

	@Option(names = "--index", description = "RediSearch index")
	private String index;
	@Option(names = "--query", description = "RediSearch query (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
	private String query = "*";
	@Option(names = "--num", description = "Limit results to number (default: ${DEFAULT-VALUE})", paramLabel = "<count>")
	private long limitNum = Limit.DEFAULT_NUM;
	@Option(names = "--offset", description = "Limit results to offset (default: ${DEFAULT-VALUE})", paramLabel = "<count>")
	private long limitOffset = Limit.DEFAULT_OFFSET;

	public String getIndex() {
		return index;
	}

	public String getQuery() {
		return query;
	}

	public RediSearchReader reader(ClientResources clientResources, RedisURI redisUri) {
		SearchOptionsBuilder builder = SearchOptions.builder();
		builder.limit(Limit.builder().num(limitNum).offset(limitOffset).build());
		RediSearchClient client = RediSearchClient.create(clientResources, redisUri);
		return new RediSearchReader(client, index, query, builder.build());
	}

	public boolean isSet() {
		return index != null;
	}

}
