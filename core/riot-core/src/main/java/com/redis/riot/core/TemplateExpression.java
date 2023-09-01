package com.redis.riot.core;

import org.springframework.expression.Expression;

public class TemplateExpression {

    private Expression expression;

    public Expression getExpression() {
        return expression;
    }

    public void setExpression(Expression expression) {
        this.expression = expression;
    }

}
