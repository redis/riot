
package com.redislabs.recharge.redis;

import java.util.Arrays;
import java.util.Map;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.recharge.IndexedPartitioner;

import lombok.extern.slf4j.Slf4j;

@SuppressWarnings({ "rawtypes", "unchecked" })
@Slf4j
public abstract class RedisWriter extends AbstractItemStreamItemWriter<Map> {

	public static final String KEY_SEPARATOR = ":";

	protected ConversionService converter = new DefaultConversionService();
	protected GenericObjectPool<StatefulRediSearchConnection<String, String>> pool;
	private Integer flushall;

	public void setFlushall(Integer flushall) {
		this.flushall = flushall;
	}

	@Override
	public void open(ExecutionContext executionContext) {
		if (IndexedPartitioner.getPartitionIndex(executionContext) == 0) {
			doOpen();
		}
		super.open(executionContext);
	}

	protected void doOpen() {
		if (flushall != null) {
			flushall();
		}
	}

	private void flushall() {
		try {
			log.warn("Flushing database in {} seconds", flushall);
			Thread.sleep(flushall * 1000);
			StatefulRediSearchConnection<String, String> connection = pool.borrowObject();
			try {
				connection.sync().flushall();
			} finally {
				pool.returnObject(connection);
			}
		} catch (Exception e) {
			log.error("Could not perform flushall", e);
		}

	}

	public void setPool(GenericObjectPool<StatefulRediSearchConnection<String, String>> pool) {
		this.pool = pool;
	}

	protected String getValues(Map record, String[] fields) {
		if (fields.length == 0) {
			return null;
		}
		String[] values = new String[fields.length];
		Arrays.setAll(values, index -> converter.convert(record.get(fields[index]), String.class));
		return join(values);
	}

	protected String join(String... values) {
		return String.join(KEY_SEPARATOR, values);
	}

	protected void convert(Map record) {
		for (Object key : record.keySet()) {
			Object value = record.get(key);
			record.put(key, converter.convert(value, String.class));
		}
	}

}
