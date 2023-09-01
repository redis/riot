package com.redis.riot.cli;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.redis.spring.batch.util.DoubleRange;
import com.redis.spring.batch.util.IntRange;

class TypeConverterTests {

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

}
