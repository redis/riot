package com.redislabs.riot.redis.reader;

import java.util.Map;

public class KeyFieldValueMapProcessor extends KeyValueMapProcessor {

	private FieldExtractor keyFieldExtractor;

	public KeyFieldValueMapProcessor(FieldExtractor keyFieldExtractor) {
		this.keyFieldExtractor = keyFieldExtractor;
	}

	@Override
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Map process(KeyValue item) throws Exception {
		Map map = super.process(item);
		if (map != null) {
			map.putAll(keyFieldExtractor.getFields(item.getKey()));
		}
		return map;
	}

}
