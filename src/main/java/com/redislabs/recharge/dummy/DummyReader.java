package com.redislabs.recharge.dummy;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

@SuppressWarnings("rawtypes")
public class DummyReader extends AbstractItemCountingItemStreamItemReader<Map> {

	public DummyReader() {
		setName(ClassUtils.getShortName(DummyReader.class));
	}

	@Override
	protected void doOpen() throws Exception {
		// do nothing
	}

	@Override
	@SuppressWarnings("unchecked")
	protected Map doRead() throws Exception {
		Map output = new LinkedHashMap<>();
		output.put("currentItemCount", getCurrentItemCount());
		return output;
	}

	@Override
	protected void doClose() throws Exception {
		// do nothing
	}

}
