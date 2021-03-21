package com.redislabs.riot.processor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionInvocationTargetException;
import org.springframework.util.Assert;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class SpelProcessor implements ItemProcessor<Map<String, Object>, Map<String, Object>> {

    private final Map<String, Expression> expressions;
    private final EvaluationContext context;
    private final AtomicLong index;

    public SpelProcessor(EvaluationContext context, Map<String, Expression> expressions) {
        Assert.notNull(context, "A SpEL evaluation context is required.");
        Assert.notEmpty(expressions, "At least one field is required.");
        this.index = new AtomicLong();
        this.context = context;
        this.context.setVariable("index", index);
        this.expressions = expressions;
    }

    @Override
    public Map<String, Object> process(Map<String, Object> item) {
        Map<String, Object> map = new HashMap<>(item);
        synchronized (context) {
            for (String field : expressions.keySet()) {
                Expression expression = expressions.get(field);
                try {
                    Object value = expression.getValue(context, map);
                    if (value == null) {
                        map.remove(field);
                    } else {
                        map.put(field, value);
                    }
                } catch (ExpressionInvocationTargetException e) {
                    log.error("Error while evaluating field {}", field, e);
                    throw e;
                }
            }
            index.incrementAndGet();
        }
        return map;
    }

}
