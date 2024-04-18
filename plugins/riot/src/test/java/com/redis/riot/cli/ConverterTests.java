package com.redis.riot.cli;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.redis.spring.batch.gen.Range;

class ConverterTests {

	@Test
	void parse() {
		RangeTypeConverter<Range> converter = new RangeTypeConverter<>(Range::new);
		assertRange(converter.convert(":100"), 0, 100);
		assertRange(converter.convert("0:"), 0, Integer.MAX_VALUE);
		assertRange(converter.convert("0:*"), 0, Integer.MAX_VALUE);
		assertRange(converter.convert(":*"), 0, Integer.MAX_VALUE);
		assertRange(converter.convert(":"), 0, Integer.MAX_VALUE);
		assertRange(converter.convert("100"), 100, 100);
		assertRange(converter.convert("100"), 100, 100);
		assertRange(converter.convert("1234567890:1234567890"), 1234567890, 1234567890);
	}

	private void assertRange(Range range, int min, int max) {
		Assertions.assertEquals(min, range.getMin());
		Assertions.assertEquals(max, range.getMax());
	}

}
