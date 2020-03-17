
package com.redislabs.riot.redis.reader;

import java.util.Iterator;
import java.util.Map;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.search.SearchOptions;
import com.redislabs.lettusearch.search.SearchResult;

public class RediSearchDocumentReader extends AbstractItemCountingItemStreamItemReader<Map<String, Object>> {

	private StatefulRediSearchConnection<String, String> connection;
	private String index;
	private String query;
	private SearchOptions options;
	private FieldExtractor extractor;
	private Object lock = new Object();
	private Iterator<SearchResult<String, String>> resultIterator;

	public RediSearchDocumentReader(StatefulRediSearchConnection<String, String> connection, String index, String query,
			SearchOptions options, FieldExtractor extractor) {
		setName(ClassUtils.getShortName(RediSearchDocumentReader.class));
		this.connection = connection;
		this.index = index;
		this.query = query;
		this.options = options;
		this.extractor = extractor;
	}

	@Override
	protected void doOpen() throws Exception {
		Assert.state(connection == null, "Cannot open an already open ItemReader, call close first");
		resultIterator = connection.sync().search(index, query, options).iterator();
	}

	@Override
	protected void doClose() throws Exception {
		synchronized (lock) {
			connection.close();
			connection = null;
			resultIterator = null;
		}
	}

	@SuppressWarnings({ "rawtypes" })
	@Override
	protected Map doRead() throws Exception {
		synchronized (lock) {
			if (resultIterator.hasNext()) {
				SearchResult<String, String> result = resultIterator.next();
				Map<String, String> fields = extractor.getFields(result.documentId());
				result.putAll(fields);
				return result;
			}
		}
		return null;
	}

}
