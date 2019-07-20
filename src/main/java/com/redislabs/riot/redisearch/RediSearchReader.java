
package com.redislabs.riot.redisearch;

import java.util.Iterator;
import java.util.Map;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.search.SearchOptions;
import com.redislabs.lettusearch.search.SearchResult;

public class RediSearchReader extends AbstractItemCountingItemStreamItemReader<Map<String, Object>> {

	private GenericObjectPool<StatefulRediSearchConnection<String, String>> pool;
	private Object lock = new Object();
	private StatefulRediSearchConnection<String, String> connection;

	private String index;
	private String query;
	private SearchOptions options;
	private Iterator<SearchResult<String, String>> resultIterator;

	public RediSearchReader(GenericObjectPool<StatefulRediSearchConnection<String, String>> pool, String index,
			String query, SearchOptions options) {
		setName(ClassUtils.getShortName(RediSearchReader.class));
		this.pool = pool;
		this.index = index;
		this.query = query;
		this.options = options;
	}

	@Override
	protected void doOpen() throws Exception {
		Assert.state(connection == null, "Cannot open an already open ItemReader, call close first");
		connection = pool.borrowObject();
		resultIterator = connection.sync().search(index, query, options).iterator();
	}

	@Override
	protected void doClose() throws Exception {
		synchronized (lock) {
			pool.returnObject(connection);
			connection = null;
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
