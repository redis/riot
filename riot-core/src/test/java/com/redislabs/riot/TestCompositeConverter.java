package com.redislabs.riot;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.core.convert.support.ConverterFactory;

import com.redislabs.riot.convert.CompositeConverter;
import com.redislabs.riot.convert.MapFieldExtractor;

public class TestCompositeConverter {

	@Test
	public void testCompositeConverter() {
		CompositeConverter converter = new CompositeConverter(MapFieldExtractor.builder().field("myField").build(),
				ConverterFactory.getStringToNumberConverter(Double.class));
		Map<String, String> map = new HashMap<>();
		map.put("myField", "123.456");
		Assertions.assertEquals(123.456, converter.convert(map));
	}

}
