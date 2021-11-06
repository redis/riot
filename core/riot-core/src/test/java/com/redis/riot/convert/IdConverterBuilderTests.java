package com.redis.riot.convert;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.core.convert.converter.Converter;

class IdConverterBuilderTests {

	@Test
	void testNoKeyConverter() {
		String prefix = "beer";
		String idField = "id";
		Converter<Map<String, Object>, String> keyMaker = new IdConverterBuilder().prefix(prefix).build();
		Map<String, Object> map = new HashMap<>();
		String id = "123";
		map.put(idField, id);
		map.put("name", "La fin du monde");
		String key = keyMaker.convert(map);
		Assertions.assertEquals(prefix, key);
	}

	@Test
	void testSingleKeyConverter() {
		String prefix = "beer";
		String idField = "id";
		Converter<Map<String, Object>, String> keyMaker = new IdConverterBuilder().prefix(prefix).fields(idField)
				.build();
		Map<String, Object> map = new HashMap<>();
		String id = "123";
		map.put(idField, id);
		map.put("name", "La fin du monde");
		String key = keyMaker.convert(map);
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
				new IdConverterBuilder().prefix(prefix).fields("store", "sku").build().convert(map));
		String separator = "~][]:''~";
		Assertions.assertEquals(prefix + separator + store + separator + sku, new IdConverterBuilder().prefix(prefix)
				.separator(separator).fields("store", "sku").build().convert(map));
	}

}
