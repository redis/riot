package com.redis.riot.core;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.common.TemplateParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import com.redis.riot.core.function.MapFunction;
import com.redis.riot.core.function.ExpressionFunction;

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

    public static UnaryOperator<Map<String, Object>> mapOperator(EvaluationContext context, Map<String, Expression> fields) {
        Map<String, Function<Map<String, Object>, Object>> functions = new LinkedHashMap<>();
        for (Entry<String, Expression> field : fields.entrySet()) {
            functions.put(field.getKey(), new ExpressionFunction<>(context, field.getValue(), Object.class));
        }
        return new MapFunction(functions);
    }

}
