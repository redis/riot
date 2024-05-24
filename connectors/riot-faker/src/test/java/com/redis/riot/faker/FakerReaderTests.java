package com.redis.riot.faker;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

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
		SpelExpressionParser parser = new SpelExpressionParser();
		int count = 100;
		FakerItemReader reader = new FakerItemReader();
		Map<String, Expression> fields = new LinkedHashMap<>();
		fields.put("index", parser.parseExpression("index"));
		fields.put("firstName", parser.parseExpression("name.firstName"));
		fields.put("lastName", parser.parseExpression("name.lastName"));
		fields.put("thread", parser.parseExpression("thread"));
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
