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
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.util.CollectionUtils;

import com.redis.riot.core.function.ExpressionFunction;
import com.redis.riot.core.function.MapFunction;
import com.redis.spring.batch.util.BatchUtils;
import com.redis.spring.batch.util.PredicateItemProcessor;

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

    public ItemProcessor<Map<String, Object>, Map<String, Object>> processor(EvaluationContext context) {
        List<ItemProcessor<Map<String, Object>, Map<String, Object>>> processors = new ArrayList<>();
        if (!CollectionUtils.isEmpty(expressions)) {
            Map<String, Function<Map<String, Object>, Object>> functions = new LinkedHashMap<>();
            for (Entry<String, Expression> field : expressions.entrySet()) {
                functions.put(field.getKey(), new ExpressionFunction<>(context, field.getValue(), Object.class));
            }
            processors.add(new FunctionItemProcessor<>(new MapFunction(functions)));
        }
        if (filter != null) {
            Predicate<Map<String, Object>> predicate = SpelUtils.predicate(context, filter);
            processors.add(new PredicateItemProcessor<>(predicate));
        }
        return BatchUtils.processor(processors);
    }

}
