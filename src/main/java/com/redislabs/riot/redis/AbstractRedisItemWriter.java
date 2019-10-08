package com.redislabs.riot.redis;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.ClassUtils;

public abstract class AbstractRedisItemWriter extends AbstractItemStreamItemWriter<Map<String, Object>> {

	private final Logger log = LoggerFactory.getLogger(AbstractRedisItemWriter.class);

	private AtomicInteger activeThreads = new AtomicInteger(0);

	public AbstractRedisItemWriter() {
		setName(ClassUtils.getShortName(this.getClass()));
	}

	@Override
	public void write(List<? extends Map<String, Object>> items) throws Exception {
		for (Map<String, Object> item : items) {
			Map<String, Object> flatMap = new HashMap<>();
			item.forEach((k, v) -> flatMap.putAll(flatten(k, v)));
			item.clear();
			item.putAll(flatMap);
		}
		items.forEach(item -> item.forEach((k, v) -> {
			item.putAll(flatten(k, v));
		}));
		doWrite(items);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Map<String, Object> flatten(String key, Object value) {
		Map<String, Object> flatMap = new HashMap<String, Object>();
		if (value instanceof Map) {
			((Map<String, Object>) value).forEach((k, v) -> {
				flatMap.putAll(flatten(key + "." + k, v));
			});
		} else {
			if (value instanceof Collection) {
				Collection collection = (Collection) value;
				Iterator iterator = collection.iterator();
				int index = 0;
				while (iterator.hasNext()) {
					flatMap.putAll(flatten(key + "[" + index + "]", iterator.next()));
					index++;
				}
			} else {
				flatMap.put(key, value);
			}
		}
		return flatMap;
	}

	protected abstract void doWrite(List<? extends Map<String, Object>> flatMaps) throws Exception;

	@Override
	public synchronized void open(ExecutionContext executionContext) {
		int threads = activeThreads.incrementAndGet();
		log.debug("Opened Redis writer, {} active threads", threads);
		super.open(executionContext);
	}

	@Override
	public synchronized void close() {
		super.close();
		int threads = activeThreads.decrementAndGet();
		log.debug("Closed Redis writer, {} active threads", threads);
	}

	protected boolean hasActiveThreads() {
		return activeThreads.get() > 0;
	}
}
