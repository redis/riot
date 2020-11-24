package com.redislabs.riot;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.redis.support.KeyMaker;
import org.springframework.core.convert.converter.Converter;

import com.redislabs.riot.convert.MapFieldExtractor;

public class TestKeyMaker {

	@SuppressWarnings("unchecked")
	@Test
	public void testSingleKeyConverter() {
		String prefix = "beer";
		String idField = "id";
		Converter<Map<String, Object>, Object> idExtractor = MapFieldExtractor.builder().field(idField).build();
		KeyMaker<Map<String, Object>> keyMaker = KeyMaker.<Map<String, Object>>builder().prefix(prefix)
				.extractors(idExtractor).build();
		Map<String, Object> map = new HashMap<>();
		String id = "123";
		map.put(idField, id);
		map.put("name", "La fin du monde");
		Object key = keyMaker.convert(map);
		Assertions.assertEquals(prefix + KeyMaker.DEFAULT_SEPARATOR + id, key);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testMultiKeyConverter() {
		String prefix = "inventory";
		Converter<Map<String, Object>, Object> storeExtractor = MapFieldExtractor.builder().field("store").build();
		Converter<Map<String, Object>, Object> skuExtractor = MapFieldExtractor.builder().field("sku").build();
		Map<String, Object> map = new HashMap<>();
		String store = "403";
		String sku = "39323";
		map.put("store", store);
		map.put("sku", sku);
		map.put("name", "La fin du monde");
		Assertions.assertEquals(prefix + KeyMaker.DEFAULT_SEPARATOR + store + KeyMaker.DEFAULT_SEPARATOR + sku,
				KeyMaker.<Map<String, Object>>builder().prefix(prefix).extractors(storeExtractor, skuExtractor).build()
						.convert(map));
		String separator = "~][]:''~";
		Assertions.assertEquals(prefix + separator + store + separator + sku, KeyMaker.<Map<String, Object>>builder()
				.prefix(prefix).separator(separator).extractors(storeExtractor, skuExtractor).build().convert(map));
	}

}
