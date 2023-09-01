package com.redis.riot.core.faker;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.support.DataBindingMethodResolver;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.redis.riot.core.SpelUtils;

import net.datafaker.Faker;

/**
 * {@link ItemReader} that generates HashMaps using Faker.
 *
 * @author Julien Ruaux
 */
public class FakerItemReader extends AbstractItemCountingItemStreamItemReader<Map<String, Object>> {

    public static final Locale DEFAULT_LOCALE = Locale.getDefault();

    private static final String EXECUTION_CONTEXT = "ctx";

    private StandardEvaluationContext evaluationContext = new StandardEvaluationContext();

    private Map<String, Expression> fields = new LinkedHashMap<>();

    private Locale locale = DEFAULT_LOCALE;

    private boolean open;

    public FakerItemReader() {
        setName(ClassUtils.getShortName(getClass()));
    }

    public void setEvaluationContext(StandardEvaluationContext context) {
        this.evaluationContext = context;
    }

    public void setLocale(Locale locale) {
        this.locale = locale;
    }

    public void setFields(Map<String, Expression> fields) {
        this.fields = fields;
    }

    public void setStringFields(Map<String, String> stringFields) {
        Map<String, Expression> expressions = new LinkedHashMap<>();
        stringFields.forEach((k, v) -> expressions.put(k, SpelUtils.parse(v)));
        this.fields = expressions;
    }

    @Override
    protected synchronized void doOpen() throws Exception {
        if (!isOpen()) {
            Assert.notEmpty(fields, "No field specified");
            evaluationContext.addPropertyAccessor(new ReflectivePropertyAccessor());
            evaluationContext.addMethodResolver(DataBindingMethodResolver.forInstanceMethodInvocation());
            evaluationContext.setRootObject(new Faker(locale));
            evaluationContext.setVariable(EXECUTION_CONTEXT, new ExecutionContext());
            open = true;
        }
    }

    @Override
    protected Map<String, Object> doRead() throws Exception {
        Map<String, Object> map = new HashMap<>();
        fields.forEach((k, v) -> map.put(k, v.getValue(evaluationContext)));
        return map;
    }

    @Override
    protected synchronized void doClose() throws Exception {
        if (isOpen()) {
            open = false;
        }
    }

    public boolean isOpen() {
        return open;
    }

    public class ExecutionContext {

        public int getIndex() {
            return index();
        }

        public int index() {
            return getCurrentItemCount();
        }

        public Thread getThread() {
            return thread();
        }

        public Thread thread() {
            return Thread.currentThread();
        }

    }

}
