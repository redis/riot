package com.redislabs.riot.batch;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;

public class LoggingWriter extends AbstractItemStreamItemWriter<Map<String, Object>> {

	private final Logger log = LoggerFactory.getLogger(LoggingWriter.class);

	@Override
	public void write(List<? extends Map<String, Object>> items) throws Exception {
		items.forEach(item -> log.info(String.valueOf(item)));
	}
}