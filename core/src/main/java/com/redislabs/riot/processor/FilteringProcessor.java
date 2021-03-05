package com.redislabs.riot.processor;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class FilteringProcessor implements ItemProcessor<Map<String, Object>, Map<String, Object>> {

    private final StandardEvaluationContext context;
    private final List<Expression> expressions;

    public FilteringProcessor(String... filters) {
        this.context = new StandardEvaluationContext();
        context.setPropertyAccessors(Collections.singletonList(new MapAccessor()));
        SpelExpressionParser parser = new SpelExpressionParser();
        this.expressions = new ArrayList<>();
        for (String filter : filters) {
            expressions.add(parser.parseExpression(filter));
        }
    }

    @Override
    public Map<String, Object> process(Map<String, Object> item) throws Exception {
        for (Expression expression : expressions) {
            if (Boolean.FALSE.equals(expression.getValue(context, item))) {
                return null;
            }
        }
        return item;
    }
}
