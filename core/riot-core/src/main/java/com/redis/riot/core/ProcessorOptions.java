package com.redis.riot.core;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.function.Predicate;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.function.FunctionItemProcessor;
import org.springframework.context.expression.MapAccessor;
import org.springframework.expression.AccessException;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.TypedValue;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.lang.Nullable;
import org.springframework.util.CollectionUtils;

import com.redis.lettucemod.util.GeoLocation;
import com.redis.riot.core.function.ExpressionFunction;
import com.redis.riot.core.function.MapFunction;
import com.redis.spring.batch.util.BatchUtils;

public class ProcessorOptions {

    private Map<String, Expression> expressions;

    private Expression filter;

    public Map<String, Expression> getExpressions() {
        return expressions;
    }

    public void setExpressions(Map<String, Expression> expressions) {
        this.expressions = expressions;
    }

    public Expression getFilter() {
        return filter;
    }

    public void setFilter(Expression filter) {
        this.filter = filter;
    }

    public ItemProcessor<Map<String, Object>, Map<String, Object>> processor(StandardEvaluationContext context) {
        context.addPropertyAccessor(new QuietMapAccessor());
        try {
            context.registerFunction("geo", GeoLocation.class.getDeclaredMethod("toString", String.class, String.class));
        } catch (NoSuchMethodException e) {
            // ignore
        }
        List<ItemProcessor<Map<String, Object>, Map<String, Object>>> processors = new ArrayList<>();
        if (!CollectionUtils.isEmpty(expressions)) {
            Map<String, Function<Map<String, Object>, Object>> functions = new LinkedHashMap<>();
            for (Entry<String, Expression> field : expressions.entrySet()) {
                functions.put(field.getKey(), new ExpressionFunction<>(context, field.getValue(), Object.class));
            }
            processors.add(new FunctionItemProcessor<>(new MapFunction(functions)));
        }
        if (filter != null) {
            Predicate<Map<String, Object>> predicate = RiotUtils.predicate(context, filter);
            processors.add(new PredicateItemProcessor<>(predicate));
        }
        return BatchUtils.processor(processors);
    }

    /**
     * {@link org.springframework.context.expression.MapAccessor} that always returns true for canRead and does not throw
     * AccessExceptions
     *
     */
    public static class QuietMapAccessor extends MapAccessor {

        @Override
        public boolean canRead(EvaluationContext context, @Nullable Object target, String name) {
            return true;
        }

        @Override
        public TypedValue read(EvaluationContext context, @Nullable Object target, String name) {
            try {
                return super.read(context, target, name);
            } catch (AccessException e) {
                return new TypedValue(null);
            }
        }

    }

}
