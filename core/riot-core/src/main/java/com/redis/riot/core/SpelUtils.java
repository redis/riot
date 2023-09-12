package com.redis.riot.core;

import java.util.function.Predicate;

import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.common.TemplateParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;

public abstract class SpelUtils {

    private static final SpelExpressionParser parser = new SpelExpressionParser();

    private SpelUtils() {
    }

    public static Expression parse(String expressionString) {
        return parser.parseExpression(expressionString);
    }

    public static TemplateExpression parseTemplate(String expressionString) {
        TemplateExpression expression = new TemplateExpression();
        expression.setExpression(parser.parseExpression(expressionString, new TemplateParserContext()));
        return expression;
    }

    public static <T> Predicate<T> predicate(EvaluationContext context, Expression expression) {
        return t -> expression.getValue(context, t, Boolean.class);
    }

}
