package com.redis.riot.core.test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import com.redis.riot.core.ProcessorOptions;
import com.redis.riot.core.RiotUtils;
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

    @Test
    void testMapProcessor() throws Exception {
        ProcessorOptions options = new ProcessorOptions();
        Map<String, Expression> expressions = new LinkedHashMap<>();
        expressions.put("field1", RiotUtils.parse("'test:1'"));
        options.setExpressions(expressions);
        ItemProcessor<Map<String, Object>, Map<String, Object>> processor = options.processor(new StandardEvaluationContext());
        Map<String, Object> map = processor.process(new HashMap<>());
        Assertions.assertEquals("test:1", map.get("field1"));
        // Assertions.assertEquals("1", map.get("id"));
    }

}
