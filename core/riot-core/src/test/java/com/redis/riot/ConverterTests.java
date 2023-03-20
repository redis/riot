package com.redis.riot;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.core.convert.converter.Converter;

import com.redis.riot.convert.FieldExtractorFactory.MissingFieldException;
import com.redis.riot.convert.IdConverterBuilder;
import com.redis.spring.batch.common.DoubleRange;
import com.redis.spring.batch.common.IntRange;

class ConverterTests {

	private final IntRangeTypeConverter intRangeConverter = new IntRangeTypeConverter();
	private final DoubleRangeTypeConverter doubleRangeConverter = new DoubleRangeTypeConverter();

	@Test
	void intRange() throws Exception {
		IntRange range = IntRange.between(1, 5);
		Assertions.assertEquals(range, intRange(range));
		range = IntRange.is(3123123);
		Assertions.assertEquals(range, intRange(range));
		Assertions.assertEquals(IntRange.between(0, 5), intConvert(":5"));
	}

	private IntRange intRange(IntRange range) {
		return intConvert(range.toString());
	}

	private IntRange intConvert(String string) {
		return intRangeConverter.convert(string);
	}

	@Test
	void doubleRange() throws Exception {
		DoubleRange range = DoubleRange.between(1.3, 1.5);
		Assertions.assertEquals(range, doubleRange(range));
		range = DoubleRange.is(1.234);
		Assertions.assertEquals(range, doubleRange(range));
		Assertions.assertEquals(DoubleRange.between(0, 1.2343), doubleConvert(":1.2343"));
	}

	private DoubleRange doubleRange(DoubleRange range) {
		return doubleConvert(range.toString());
	}

	private DoubleRange doubleConvert(String string) {
		return doubleRangeConverter.convert(string);
	}

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

	@Test
	void testNullCheck() {
		String prefix = "inventory";
		Map<String, Object> map = new HashMap<>();
		String store = "403";
		map.put("store", store);
		map.put("sku", null);
		map.put("name", "La fin du monde");
		Converter<Map<String, Object>, String> converter = new IdConverterBuilder().prefix(prefix)
				.fields("store", "sku").build();
		Assertions.assertThrows(MissingFieldException.class, () -> converter.convert(map));
	}

}
