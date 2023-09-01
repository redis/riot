package com.redis.riot.core;

import java.util.Map;

import org.springframework.expression.Expression;

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

}
