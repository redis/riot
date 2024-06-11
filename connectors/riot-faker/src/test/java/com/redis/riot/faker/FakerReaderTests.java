package com.redis.riot.faker;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;

import com.redis.riot.core.Expression;

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
		Map<String, Expression> fields = new LinkedHashMap<>();
		fields.put("index", Expression.parse("index"));
		fields.put("firstName", Expression.parse("name.firstName"));
		fields.put("lastName", Expression.parse("name.lastName"));
		fields.put("thread", Expression.parse("thread"));
		reader.setFields(fields);
		reader.setMaxItemCount(count);
		reader.open(new ExecutionContext());
		List<Map<String, Object>> items = readAll(reader);
		reader.close();
		Assertions.assertEquals(count, items.size());
		Assertions.assertEquals(1, items.get(0).get("index"));
		Assertions.assertEquals(1, (Integer) items.get(0).get("thread"));
	}

}
