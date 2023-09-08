package com.redis.riot.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Predicate;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import io.lettuce.core.codec.StringCodec;

class ProcessorTests {

    @Test
    void keyFilter() {
        Predicate<String> predicate = KeyFilterOptions.builder().includes("foo*", "bar*").build().predicate(StringCodec.UTF8);
        Assertions.assertTrue(predicate.test("foobar"));
        Assertions.assertTrue(predicate.test("barfoo"));
        Assertions.assertFalse(predicate.test("key"));
    }

    @Test
    void testMapProcessor() throws Exception {
        ProcessorOptions options = new ProcessorOptions();
        Map<String, Expression> expressions = new LinkedHashMap<>();
        expressions.put("field1", SpelUtils.parse("'value1'"));
        expressions.put("field2", SpelUtils.parse("field1"));
        expressions.put("field3", SpelUtils.parse("1"));
        expressions.put("field4", SpelUtils.parse("2"));
        expressions.put("field5", SpelUtils.parse("field3+field4"));
        options.setExpressions(expressions);
        ItemProcessor<Map<String, Object>, Map<String, Object>> processor = options.processor(mapEvaluationContext());
        for (int index = 0; index < 10; index++) {
            Map<String, Object> result = processor.process(new HashMap<>());
            assertEquals(5, result.size());
            assertEquals("value1", result.get("field1"));
            assertEquals("value1", result.get("field2"));
            assertEquals(3, result.get("field5"));
        }
    }

    private EvaluationContext mapEvaluationContext() {
        StandardEvaluationContext context = new StandardEvaluationContext();
        context.addPropertyAccessor(new QuietMapAccessor());
        return context;
    }

    @Test
    void testMapProcessorFilter() throws Exception {
        ProcessorOptions options = new ProcessorOptions();
        options.setFilter(SpelUtils.parse("index<10"));
        ItemProcessor<Map<String, Object>, Map<String, Object>> processor = options.processor(mapEvaluationContext());
        for (int index = 0; index < 100; index++) {
            Map<String, Object> map = new HashMap<>();
            map.put("index", index);
            Map<String, Object> result = processor.process(map);
            if (index < 10) {
                Assertions.assertNotNull(result);
            } else {
                Assertions.assertNull(result);
            }
        }
    }

}
