package com.redis.riot.core;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.SimpleEvaluationContext;
import org.springframework.expression.spel.support.SimpleEvaluationContext.Builder;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.redis.spring.batch.common.IntRange;

import net.datafaker.Faker;

/**
 * {@link ItemReader} that generates HashMaps using Faker.
 *
 * @author Julien Ruaux
 */
public class FakerItemReader extends AbstractItemCountingItemStreamItemReader<Map<String, Object>> {

    private static final String FIELD_THREAD = "thread";

    public static final String FIELD_INDEX = "index";

    public static final Locale DEFAULT_LOCALE = Locale.getDefault();

    private final SpelExpressionParser parser = new SpelExpressionParser();

    private final Map<String, Expression> expressions = new LinkedHashMap<>();

    private IntRange indexRange = IntRange.from(1);

    private Locale locale = DEFAULT_LOCALE;

    private boolean includeMetadata;

    private EvaluationContext context;

    private int maxItemCount = Integer.MAX_VALUE;

    public FakerItemReader() {
        setName(ClassUtils.getShortName(getClass()));
    }

    @Override
    public void setMaxItemCount(int count) {
        super.setMaxItemCount(count);
        this.maxItemCount = count;
    }

    public int size() {
        if (maxItemCount == Integer.MAX_VALUE) {
            return -1;
        }
        return maxItemCount - getCurrentItemCount();
    }

    public FakerItemReader withIndexRange(IntRange range) {
        this.indexRange = range;
        return this;
    }

    public FakerItemReader withLocale(Locale locale) {
        this.locale = locale;
        return this;
    }

    public FakerItemReader withIncludeMetadata(boolean include) {
        this.includeMetadata = include;
        return this;
    }

    public FakerItemReader withField(String field, String expression) {
        this.expressions.put(field, parser.parseExpression(expression));
        return this;
    }

    public FakerItemReader withFields(String... fields) {
        Assert.isTrue(fields.length % 2 == 0,
                "fields.length must be a multiple of 2 and contain a sequence of field1, expression1, field2, expression2, fieldN, expressionN");
        for (int i = 0; i < fields.length; i += 2) {
            withField(fields[i], fields[i + 1]);
        }
        return this;
    }

    @Override
    protected Map<String, Object> doRead() throws Exception {
        Map<String, Object> map = new HashMap<>();
        int index = index();
        if (includeMetadata) {
            map.put(FIELD_INDEX, index);
            map.put(FIELD_THREAD, Thread.currentThread().getId());
        }
        context.setVariable(FIELD_INDEX, index);
        for (Entry<String, Expression> expression : expressions.entrySet()) {
            map.put(expression.getKey(), expression.getValue().getValue(context));
        }
        return map;
    }

    private int index() {
        return (indexRange.getMin() + getCurrentItemCount() - 1) % indexRange.getMax();
    }

    @Override
    protected void doOpen() throws Exception {
        synchronized (parser) {
            if (!isOpen()) {
                Faker faker = new Faker(locale);
                Builder contextBuilder = SimpleEvaluationContext.forPropertyAccessors(new ReflectivePropertyAccessor());
                contextBuilder.withInstanceMethods();
                contextBuilder.withRootObject(faker);
                this.context = contextBuilder.build();
            }
        }
    }

    @Override
    protected void doClose() throws Exception {
        synchronized (parser) {
            this.context = null;
        }
    }

    public boolean isOpen() {
        return context != null;
    }

}
