package com.redislabs.riot.gen;

import com.github.javafaker.Faker;
import lombok.Builder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.support.BoundedItemReader;
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
public class FakerItemReader extends AbstractItemCountingItemStreamItemReader<Map<String, Object>> implements BoundedItemReader<Map<String, Object>> {

    public final static String FIELD_INDEX = "index";

    private final Locale locale;
    private final boolean includeMetadata;
    private final Map<String, String> spelFields;
    private final long start;
    private final long end;
    private final long sleep;

    private EvaluationContext context;
    private Map<String, Expression> expressions;

    @Builder
    public FakerItemReader(Locale locale, boolean includeMetadata, Map<String, String> fields, long start, long end, long sleep) {
        Assert.notNull(fields, "Fields are required.");
        Assert.isTrue(end > start, "End index must be strictly greater than start index");
        setName(ClassUtils.getShortName(getClass()));
        setMaxItemCount(Math.toIntExact(end - start));
        this.locale = locale;
        this.includeMetadata = includeMetadata;
        this.spelFields = fields;
        this.start = start;
        this.end = end;
        this.sleep = sleep;
    }

    @Override
    public Long size() {
        return end - start;
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
    protected Map<String, Object> doRead() throws InterruptedException {
        if (sleep > 0) {
            Thread.sleep(sleep);
        }
        Map<String, Object> map = new HashMap<>();
        if (includeMetadata) {
            map.put(FIELD_INDEX, index());
        }
        context.setVariable(FIELD_INDEX, index());
        for (String field : expressions.keySet()) {
            map.put(field, expressions.get(field).getValue(context));
        }
        return map;
    }

    private long index() {
        return start + (getCurrentItemCount() % (end - start));
    }

}
