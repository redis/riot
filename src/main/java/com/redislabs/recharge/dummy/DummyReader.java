package com.redislabs.recharge.dummy;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

public class DummyReader extends AbstractItemCountingItemStreamItemReader<Map<String, Object>> {

	public DummyReader() {
		setName(ClassUtils.getShortName(DummyReader.class));
	}

	@Override
	protected void doOpen() throws Exception {
		// do nothing
	}

	@Override
	protected Map<String, Object> doRead() throws Exception {
		Map<String, Object> output = new LinkedHashMap<>();
		output.put("currentItemCount", getCurrentItemCount());
		return output;
	}

	@Override
	protected void doClose() throws Exception {
		// do nothing
	}

}
