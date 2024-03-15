package com.redis.riot.core;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.redis.riot.core.function.StringToMapFunction;
import com.redis.riot.core.function.StructToMapFunction;
import com.redis.spring.batch.common.DataType;
import com.redis.spring.batch.common.KeyValue;

class FunctionTests {

    @Test
    void keyValueToMap() {
        StructToMapFunction function = new StructToMapFunction();
        KeyValue<String> string = new KeyValue<>();
        string.setKey("beer:1");
        string.setType(DataType.STRING);
        String value = "sdfsdf";
        string.setValue(value);
        Map<String, ?> stringMap = function.apply(string);
        Assertions.assertEquals(value, stringMap.get(StringToMapFunction.DEFAULT_KEY));
        KeyValue<String> hash = new KeyValue<>();
        hash.setKey("beer:2");
        hash.setType(DataType.HASH);
        Map<String, String> map = new HashMap<>();
        map.put("field1", "value1");
        hash.setValue(map);
        Map<String, ?> hashMap = function.apply(hash);
        Assertions.assertEquals("value1", hashMap.get("field1"));
    }

}
