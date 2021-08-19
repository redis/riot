package com.redislabs.riot;

import com.redislabs.riot.processor.KeyValueProcessor;
import lombok.Data;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.redis.support.KeyValue;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.common.TemplateParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import picocli.CommandLine.Option;

@Data
public class KeyValueProcessorOptions {

    @Option(names = "--key-process", description = "SpEL expression to transform each key", paramLabel = "<exp>")
    private String keyProcessor;

    public <T extends KeyValue<?>> ItemProcessor<T, T> processor(RedisOptions sourceRedis, RedisOptions targetRedis) {
        if (keyProcessor == null) {
            return null;
        }
        SpelExpressionParser parser = new SpelExpressionParser();
        Expression expression = parser.parseExpression(keyProcessor, new TemplateParserContext());
        EvaluationContext context = new StandardEvaluationContext();
        context.setVariable("src", sourceRedis.uris().get(0));
        context.setVariable("dest", targetRedis.uris().get(0));
        return new KeyValueProcessor<>(expression, context);
    }

}
