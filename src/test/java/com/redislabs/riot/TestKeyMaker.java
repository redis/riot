package com.redislabs.riot;

import com.redislabs.riot.convert.KeyMaker;
import com.redislabs.riot.convert.field.FieldExtractor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.core.convert.converter.Converter;

import java.util.HashMap;
import java.util.Map;

public class TestKeyMaker {

    @Test
    public void testSingleKeyConverter() {
        String prefix = "beer";
        String idField = "id";
        Converter<Map<String, String>, String> idExtractor = FieldExtractor.builder().field(idField).build();
        KeyMaker<Map<String, String>> keyMaker = KeyMaker.<Map<String, String>>builder().prefix(prefix).extractors(idExtractor).build();
        Map<String, String> map = new HashMap<>();
        String id = "123";
        map.put(idField, id);
        map.put("name", "La fin du monde");
        String key = keyMaker.convert(map);
        Assertions.assertEquals(prefix + KeyMaker.DEFAULT_SEPARATOR + id, key);
    }

    @Test
    public void testMultiKeyConverter() {
        String prefix = "inventory";
        Converter<Map<String, String>, String> storeExtractor = FieldExtractor.builder().field("store").build();
        Converter<Map<String, String>, String> skuExtractor = FieldExtractor.builder().field("sku").build();
        Map<String, String> map = new HashMap<>();
        String store = "403";
        String sku = "39323";
        map.put("store", store);
        map.put("sku", sku);
        map.put("name", "La fin du monde");
        Assertions.assertEquals(prefix + KeyMaker.DEFAULT_SEPARATOR + store + KeyMaker.DEFAULT_SEPARATOR + sku, KeyMaker.<Map<String, String>>builder().prefix(prefix).extractors(storeExtractor, skuExtractor).build().convert(map));
        String separator = "~][]:''~";
        Assertions.assertEquals(prefix + separator + store + separator + sku, KeyMaker.<Map<String, String>>builder().prefix(prefix).separator(separator).extractors(storeExtractor, skuExtractor).build().convert(map));
    }

}
