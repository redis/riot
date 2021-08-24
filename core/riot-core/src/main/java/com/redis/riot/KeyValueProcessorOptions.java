package com.redis.riot;

import com.redis.riot.processor.CompositeItemStreamItemProcessor;
import com.redis.riot.processor.KeyValueKeyProcessor;
import com.redis.riot.processor.KeyValueTTLProcessor;
import lombok.Data;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.redis.support.KeyValue;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.common.TemplateParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import picocli.CommandLine.Option;

import java.util.ArrayList;
import java.util.List;

@Data
public class KeyValueProcessorOptions {

    @Option(names = "--key-process", description = "SpEL expression to transform each key", paramLabel = "<exp>")
    private String keyProcessor;
    @Option(names = "--ttl-process", description = "SpEL expression to transform each key TTL", paramLabel = "<exp>")
    private String ttlProcessor;

    public <T extends KeyValue<?>> ItemProcessor<T, T> processor(RedisOptions sourceRedis, RedisOptions targetRedis) {
        SpelExpressionParser parser = new SpelExpressionParser();
        List<ItemProcessor<T, T>> processors = new ArrayList<>();
        if (keyProcessor != null) {
            EvaluationContext context = new StandardEvaluationContext();
            context.setVariable("src", sourceRedis.uris().get(0));
            context.setVariable("dest", targetRedis.uris().get(0));
            processors.add(new KeyValueKeyProcessor<>(parser.parseExpression(keyProcessor, new TemplateParserContext()), context));
        }
        if (ttlProcessor != null) {
            processors.add(new KeyValueTTLProcessor<>(parser.parseExpression(ttlProcessor), new StandardEvaluationContext()));
        }
        return CompositeItemStreamItemProcessor.delegates(processors);
    }

}
