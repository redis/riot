
package com.redislabs.riot.redisearch;

import java.util.Iterator;
import java.util.Map;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.search.SearchOptions;
import com.redislabs.lettusearch.search.SearchResult;

public class RediSearchItemReader extends AbstractItemCountingItemStreamItemReader<Map<String, Object>> {

	private RediSearchClient client;
	private StatefulRediSearchConnection<String, String> connection;
	private String index;
	private String query;
	private SearchOptions options;
	private Object lock = new Object();
	private Iterator<SearchResult<String, String>> resultIterator;

	public RediSearchItemReader(RediSearchClient client, String index, String query, SearchOptions options) {
		setName(ClassUtils.getShortName(RediSearchItemReader.class));
		this.client = client;
		this.index = index;
		this.query = query;
		this.options = options;
	}

	@Override
	protected void doOpen() throws Exception {
		Assert.state(connection == null, "Cannot open an already open ItemReader, call close first");
		connection = client.connect();
		resultIterator = connection.sync().search(index, query, options).iterator();
	}

	@Override
	protected void doClose() throws Exception {
		synchronized (lock) {
			connection.close();
			connection = null;
			client.shutdown();
			client.getResources().shutdown().get();
			resultIterator = null;
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	protected Map<String, Object> doRead() throws Exception {
		synchronized (lock) {
			if (resultIterator.hasNext()) {
				return (Map) resultIterator.next();
			}
		}
		return null;
	}

}
