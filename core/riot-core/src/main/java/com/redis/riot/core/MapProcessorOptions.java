package com.redis.riot.core;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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

import com.redis.riot.core.function.ExpressionFunction;
import com.redis.riot.core.function.MapFunction;
import com.redis.riot.core.function.MapToFieldFunction;
import com.redis.riot.core.function.RegexNamedGroupFunction;
import com.redis.riot.core.function.ToMapFunction;
import com.redis.spring.batch.util.BatchUtils;
import com.redis.spring.batch.util.PredicateItemProcessor;

public class MapProcessorOptions {

    private Map<String, Expression> expressions;

    private Map<String, Pattern> regexes;

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

    public Map<String, Pattern> getRegexes() {
        return regexes;
    }

    public void setRegexes(Map<String, Pattern> regexes) {
        this.regexes = regexes;
    }

    public Expression getFilter() {
        return filter;
    }

    public void setFilter(Expression filter) {
        this.filter = filter;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public ItemProcessor<Map<String, Object>, Map<String, Object>> processor() {
        List<ItemProcessor<Map<String, Object>, ? extends Map<String, ?>>> processors = new ArrayList<>();
        StandardEvaluationContext context = evaluationContextOptions.evaluationContext();
        context.addPropertyAccessor(new QuietMapAccessor());
        if (!CollectionUtils.isEmpty(expressions)) {
            processors.add(new FunctionItemProcessor<>(mapFunction(context)));
        }
        if (!CollectionUtils.isEmpty(regexes)) {
            List<Function<Map<String, Object>, Map<String, Object>>> functions = new ArrayList<>();
            functions.add(Function.identity());
            functions.addAll((List) regexFunctions());
            processors.add(new FunctionItemProcessor<>(new ToMapFunction<>(functions)));
        }
        if (filter != null) {
            Predicate<Map<String, Object>> predicate = SpelUtils.predicate(context, filter);
            processors.add(new PredicateItemProcessor<>(predicate));
        }
        return BatchUtils.processor(processors);
    }

    private List<Function<Map<String, Object>, Map<String, String>>> regexFunctions() {
        return regexes.entrySet().stream().map(e -> toFieldFunction(e.getKey(), e.getValue())).collect(Collectors.toList());
    }

    private Function<Map<String, Object>, Map<String, String>> toFieldFunction(String key, Pattern pattern) {
        return new MapToFieldFunction(key).andThen(String::valueOf).andThen(new RegexNamedGroupFunction(pattern));
    }

    private MapFunction mapFunction(EvaluationContext context) {
        Map<String, Function<Map<String, Object>, Object>> functions = new LinkedHashMap<>();
        for (Entry<String, Expression> field : expressions.entrySet()) {
            functions.put(field.getKey(), new ExpressionFunction<>(context, field.getValue(), Object.class));
        }
        return new MapFunction(functions);
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
