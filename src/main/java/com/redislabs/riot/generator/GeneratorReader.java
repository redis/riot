package com.redislabs.riot.generator;

import com.github.javafaker.Faker;
import lombok.Builder;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.SimpleEvaluationContext;
import org.springframework.util.ClassUtils;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

public class GeneratorReader extends AbstractItemCountingItemStreamItemReader<Map<String, Object>> {

    public final static String FIELD_INDEX = "index";

    private final Locale locale;
    private final boolean includeMetadata;
    private final Map<String, String> fields;

    private EvaluationContext context;
    private Map<String, Expression> expressions;

    @Builder
    public GeneratorReader(Locale locale, boolean includeMetadata, Map<String, String> fields) {
        setName(ClassUtils.getShortName(getClass()));
        this.locale = locale;
        this.includeMetadata = includeMetadata;
        this.fields = fields;
    }

    @Override
    protected void doOpen() {
        this.context = new SimpleEvaluationContext.Builder(new ReflectivePropertyAccessor()).withInstanceMethods().withRootObject(new Faker(locale)).build();
        SpelExpressionParser parser = new SpelExpressionParser();
        this.expressions = fields.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> parser.parseExpression(e.getValue())));
    }

    @Override
    protected void doClose() {
        this.expressions.clear();
        this.context = null;
    }

    @Override
    protected Map<String, Object> doRead() {
        Map<String, Object> map = new HashMap<>();
        if (includeMetadata) {
            map.put(FIELD_INDEX, getCurrentItemCount());
        }
        context.setVariable("index", getCurrentItemCount());
        for (String field : expressions.keySet()) {
            map.put(field, expressions.get(field).getValue(context));
        }
        return map;
    }

}
