package com.redis.riot.core;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.redis.riot.core.convert.FieldExtractorFactory.MissingFieldException;
import com.redis.riot.core.convert.IdConverterBuilder;

class ConverterTests {

	@Test
	void testNoKeyConverter() {
		String prefix = "beer";
		String idField = "id";
		Function<Map<String, Object>, String> keyMaker = new IdConverterBuilder().prefix(prefix).build();
		Map<String, Object> map = new HashMap<>();
		String id = "123";
		map.put(idField, id);
		map.put("name", "La fin du monde");
		String key = keyMaker.apply(map);
		Assertions.assertEquals(prefix, key);
	}

	@Test
	void testSingleKeyConverter() {
		String prefix = "beer";
		String idField = "id";
		Function<Map<String, Object>, String> keyMaker = new IdConverterBuilder().prefix(prefix).fields(idField)
				.build();
		Map<String, Object> map = new HashMap<>();
		String id = "123";
		map.put(idField, id);
		map.put("name", "La fin du monde");
		String key = keyMaker.apply(map);
		Assertions.assertEquals(prefix + IdConverterBuilder.DEFAULT_SEPARATOR + id, key);
	}

	@Test
	void testMultiKeyConverter() {
		String prefix = "inventory";
		Map<String, Object> map = new HashMap<>();
		String store = "403";
		String sku = "39323";
		map.put("store", store);
		map.put("sku", sku);
		map.put("name", "La fin du monde");
		Assertions.assertEquals(
				prefix + IdConverterBuilder.DEFAULT_SEPARATOR + store + IdConverterBuilder.DEFAULT_SEPARATOR + sku,
				new IdConverterBuilder().prefix(prefix).fields("store", "sku").build().apply(map));
		String separator = "~][]:''~";
		Assertions.assertEquals(prefix + separator + store + separator + sku,
				new IdConverterBuilder().prefix(prefix).separator(separator).fields("store", "sku").build().apply(map));
	}

	@Test
	void testNullCheck() {
		String prefix = "inventory";
		Map<String, Object> map = new HashMap<>();
		String store = "403";
		map.put("store", store);
		map.put("sku", null);
		map.put("name", "La fin du monde");
		Function<Map<String, Object>, String> converter = new IdConverterBuilder().prefix(prefix).fields("store", "sku")
				.build();
		Assertions.assertThrows(MissingFieldException.class, () -> converter.apply(map));
	}

}
