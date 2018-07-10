package com.redislabs.recharge.dummy;

import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

public class DummyItemReader extends AbstractItemCountingItemStreamItemReader<Map<String, Object>> {

	public static final String FIELD = "field";

	private long index;

	public DummyItemReader() {
		setName(ClassUtils.getShortName(DummyItemReader.class));
	}

	@Override
	protected Map<String, Object> doRead() throws Exception {
		Map<String, Object> map = new HashMap<>();
		map.put(FIELD, index++);
		return map;
	}

	@Override
	protected void doOpen() throws Exception {
	}

	@Override
	protected void doClose() throws Exception {
	}

}
