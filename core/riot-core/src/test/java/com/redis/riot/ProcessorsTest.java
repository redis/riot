package com.redis.riot;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import com.redis.riot.processor.KeyValueKeyProcessor;
import com.redis.spring.batch.support.KeyValue;

public class ProcessorsTest {

    @Test
    public void testKeyValueProcessor() {
        SpelExpressionParser parser = new SpelExpressionParser();
        Expression expression = parser.parseExpression("'pre:'.concat(key).concat(':post')");
        KeyValueKeyProcessor<KeyValue<String, String>> processor = new KeyValueKeyProcessor<>(expression, new StandardEvaluationContext());
        KeyValue<String, String> keyValue = processor.process(new KeyValue<>("123"));
        Assertions.assertEquals("pre:123:post", keyValue.getKey());
    }

    @Test
    public void testKeyValueProcessorOptions() throws Exception {
        KeyValueProcessorOptions options = new KeyValueProcessorOptions();
        options.setKeyProcessor("#{#src.database}:#{key}:#{#src.host}");
        RedisOptions src = new RedisOptions();
        src.setDatabase(14);
        src.setHost("srchost");
        RedisOptions dest = new RedisOptions();
        dest.setDatabase(15);
        dest.setHost("desthost");
        ItemProcessor<KeyValue<String, String>, KeyValue<String, String>> processor = options.processor(src, dest);
        KeyValue<String, String> keyValue = processor.process(new KeyValue<>("123"));
        Assertions.assertEquals("14:123:srchost", keyValue.getKey());
    }
}
