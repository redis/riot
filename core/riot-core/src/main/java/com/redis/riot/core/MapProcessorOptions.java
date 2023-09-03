package com.redis.riot.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

import com.redis.spring.batch.util.BatchUtils;
import com.redis.spring.batch.util.PredicateItemProcessor;

public class MapProcessorOptions {

    private Map<String, Expression> expressions;

    private Expression filter;

    private EvaluationContextOptions evaluationContextOptions = new EvaluationContextOptions();

    public EvaluationContextOptions getEvaluationContextOptions() {
        return evaluationContextOptions;
    }

    public void setEvaluationContextOptions(EvaluationContextOptions evaluationContextOptions) {
        this.evaluationContextOptions = evaluationContextOptions;
    }

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

    public ItemProcessor<Map<String, Object>, Map<String, Object>> processor() {
        List<ItemProcessor<Map<String, Object>, Map<String, Object>>> processors = new ArrayList<>();
        StandardEvaluationContext context = evaluationContextOptions.evaluationContext();
        context.addPropertyAccessor(new QuietMapAccessor());
        if (!CollectionUtils.isEmpty(expressions)) {
            processors.add(new FunctionItemProcessor<>(SpelUtils.mapOperator(context, expressions)));
        }
        if (filter != null) {
            Predicate<Map<String, Object>> predicate = SpelUtils.predicate(context, filter);
            processors.add(new PredicateItemProcessor<>(predicate));
        }
        return BatchUtils.processor(processors);
    }

    /**
     * {@link org.springframework.context.expression.MapAccessor} that always returns true for canRead and does not throw
     * AccessExceptions
     *
     * @author Julien Ruaux
     */
    private static class QuietMapAccessor extends MapAccessor {

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
