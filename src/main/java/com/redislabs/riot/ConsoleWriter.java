package com.redislabs.riot;

import java.util.List;
import java.util.Map;

import org.springframework.batch.item.support.AbstractItemStreamItemWriter;

public class ConsoleWriter extends AbstractItemStreamItemWriter<Map<String, Object>> {

	@Override
	public void write(List<? extends Map<String, Object>> items) throws Exception {
		items.forEach(item -> System.out.println(item));
	}

}
