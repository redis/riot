package com.redis.riot.faker;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;

class FakerReaderTests {

	public static <T> List<T> readAll(ItemReader<T> reader) throws Exception {
		List<T> list = new ArrayList<>();
		T element;
		while ((element = reader.read()) != null) {
			list.add(element);
		}
		return list;
	}

	@Test
	void fakerReader() throws Exception {
		int count = 100;
		FakerItemReader reader = new FakerItemReader();
		Map<String, String> fields = new LinkedHashMap<>();
		fields.put("firstName", "Name.first_name");
		fields.put("lastName", "Name.last_name");
		reader.setExpressions(fields);
		reader.setMaxItemCount(count);
		reader.open(new ExecutionContext());
		List<Map<String, Object>> items = readAll(reader);
		reader.close();
		Assertions.assertEquals(count, items.size());
		Assertions.assertTrue(items.get(0).containsKey("firstName"));
		Assertions.assertTrue(items.get(0).containsKey("lastName"));
	}

}
