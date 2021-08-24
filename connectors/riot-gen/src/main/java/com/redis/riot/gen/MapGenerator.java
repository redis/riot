package com.redis.riot.gen;

import com.github.javafaker.Faker;
import lombok.Builder;
import lombok.NonNull;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.SimpleEvaluationContext;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

public class MapGenerator implements Generator<Map<String,Object>> {

    public final static String FIELD_INDEX = "index";

    private final SimpleEvaluationContext context;
    private final Map<String, Expression> expressions;

    @Builder
    private MapGenerator(Locale locale, @NonNull Map<String, String> fields) {
        Faker faker = new Faker(locale == null ? Locale.getDefault() : locale);
        this.context = new SimpleEvaluationContext.Builder(new ReflectivePropertyAccessor()).withInstanceMethods().withRootObject(faker).build();
        SpelExpressionParser parser = new SpelExpressionParser();
        this.expressions = fields.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> parser.parseExpression(e.getValue())));
    }

    @Override
    public Map<String, Object> next(long index) {
        Map<String, Object> map = new HashMap<>();
        context.setVariable(FIELD_INDEX, index);
        for (String field : expressions.keySet()) {
            map.put(field, expressions.get(field).getValue(context));
        }
        return map;
    }
}
