package com.redislabs.riot;

import java.util.*;

import com.redislabs.riot.convert.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestConverters {

    @Test
    public void testCompositeConverter() {
        CompositeConverter converter = new CompositeConverter(FieldExtractor.builder().field("myField").build(), new ObjectToNumberConverter<>(Double.class));
        Map<String, String> map = new HashMap<>();
        map.put("myField", "123.456");
        Assertions.assertEquals(123.456, converter.convert(map));
    }

    @Test
    public void testMapFilteringConverter() {
        Map<String, Object> source = new LinkedHashMap<>();
        source.put("field1", "value1");
        source.put("field2", "value2");
        source.put("field3", "value3");
        Map<String, String> include1 = MapFilteringConverter.<String, String>builder().includes("field1").build().convert(new HashMap<>(source));
        Assertions.assertEquals(1, include1.size());
        Assertions.assertEquals("value1", include1.get("field1"));
        Map<String, String> exclude1 = MapFilteringConverter.<String, String>builder().excludes("field1").build().convert(new HashMap<>(source));
        Assertions.assertEquals(2, exclude1.size());
        Assertions.assertEquals("value2", exclude1.get("field2"));
        Assertions.assertEquals("value3", exclude1.get("field3"));
        Map<String, String> include2 = MapFilteringConverter.<String, String>builder().includes("field1", "field2").excludes("field2").build().convert(new HashMap<>(source));
        Assertions.assertEquals(1, include2.size());
        Assertions.assertEquals("value1", include2.get("field1"));
    }

    @Test
    public void testNestedKeys() {
        Map<String, Object> map = map("1", map("1", "1.1", "2", "1.2"), "2", map("1", "2.1", "2", "2.2"));
        Map<String, Object> expected = new HashMap<>();
        expected.putAll(map("1.1", "1.1", "1.2", "1.2"));
        expected.putAll(map("2.1", "2.1", "2.2", "2.2"));
        MapFlattener<Object> flattener = new MapFlattener<>(s -> s);
        Assertions.assertEquals(expected, flattener.convert(map));
    }

    @Test
    public void testIndexedKeys() {
        Map<String, Object> map = map("1", Arrays.asList("1.1", "1.2"), "2", Arrays.asList("2.1", "2.2"));
        Map<String, Object> expected = new HashMap<>();
        expected.putAll(map("1[0]", "1.1", "1[1]", "1.2"));
        expected.putAll(map("2[0]", "2.1", "2[1]", "2.2"));
        MapFlattener<Object> flattener = new MapFlattener<>(s -> s);
        Assertions.assertEquals(expected, flattener.convert(map));
    }

    @Test
    public void testMixedValues() {
        Map<String, Object> map = map("1", Arrays.asList("1.1", "1.2"), "2", map("1", "2.1", "2", "2.2"));
        Map<String, Object> expected = new HashMap<>();
        expected.putAll(map("1[0]", "1.1", "1[1]", "1.2"));
        expected.putAll(map("2.1", "2.1", "2.2", "2.2"));
        MapFlattener<Object> flattener = new MapFlattener<>(s -> s);
        Assertions.assertEquals(expected, flattener.convert(map));
    }

    private Map<String, Object> map(String field1, Object value1, String field2, Object value2) {
        Map<String, Object> map = new HashMap<>();
        map.put(field1, value1);
        map.put(field2, value2);
        return map;
    }

}
