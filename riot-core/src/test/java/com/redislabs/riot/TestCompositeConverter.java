package com.redislabs.riot;

import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.core.convert.support.ConverterFactory;

import com.redislabs.riot.convert.CompositeConverter;
import com.redislabs.riot.convert.FieldExtractor;

public class TestCompositeConverter {

	@Test
	public void testCompositeConverter() {
		CompositeConverter converter = new CompositeConverter(FieldExtractor.builder().field("myField").build(),
				ConverterFactory.getStringToNumberConverter(Double.class));
		Assertions.assertEquals(123.456, converter.convert(Map.of("myField", "123.456")));
	}

}
