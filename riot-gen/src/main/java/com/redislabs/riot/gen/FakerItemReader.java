package com.redislabs.riot.gen;

import com.github.javafaker.Faker;
import lombok.Builder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.SimpleEvaluationContext;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * {@link ItemReader} that generates HashMaps using Faker.
 *
 * @author Julien Ruaux
 */
public class FakerItemReader extends AbstractItemCountingItemStreamItemReader<Map<String, Object>> {

    public final static String FIELD_INDEX = "index";

    private final Locale locale;
    private final boolean includeMetadata;
    private final Map<String, String> spelFields;

    private EvaluationContext context;
    private Map<String, Expression> expressions;

    @Builder
    public FakerItemReader(Locale locale, boolean includeMetadata, Map<String, String> fields) {
        Assert.notNull(fields, "Fields are required.");
        setName(ClassUtils.getShortName(getClass()));
        this.locale = locale;
        this.includeMetadata = includeMetadata;
        this.spelFields = fields;
    }

    @Override
    protected void doOpen() {
        this.context = new SimpleEvaluationContext.Builder(new ReflectivePropertyAccessor()).withInstanceMethods().withRootObject(new Faker(locale())).build();
        SpelExpressionParser parser = new SpelExpressionParser();
        this.expressions = spelFields.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> parser.parseExpression(e.getValue())));
    }

    private Locale locale() {
        if (locale == null) {
            return Locale.getDefault();
        }
        return locale;
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
        context.setVariable(FIELD_INDEX, getCurrentItemCount());
        for (String field : expressions.keySet()) {
            map.put(field, expressions.get(field).getValue(context));
        }
        return map;
    }

}
