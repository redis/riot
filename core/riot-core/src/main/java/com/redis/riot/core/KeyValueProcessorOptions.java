package com.redis.riot.core;

import org.springframework.expression.Expression;

public class KeyValueProcessorOptions {

    private TemplateExpression keyExpression;

    private Expression ttlExpression;

    private boolean dropTtl;

    private Expression typeExpression;

    private boolean dropStreamMessageId;

    public boolean isDropStreamMessageId() {
        return dropStreamMessageId;
    }

    public void setDropStreamMessageId(boolean dropStreamMessageId) {
        this.dropStreamMessageId = dropStreamMessageId;
    }

    public Expression getTypeExpression() {
        return typeExpression;
    }

    public void setTypeExpression(Expression expression) {
        this.typeExpression = expression;
    }

    public boolean isDropTtl() {
        return dropTtl;
    }

    public void setDropTtl(boolean dropTtl) {
        this.dropTtl = dropTtl;
    }

    public TemplateExpression getKeyExpression() {
        return keyExpression;
    }

    public void setKeyExpression(TemplateExpression expression) {
        this.keyExpression = expression;
    }

    public Expression getTtlExpression() {
        return ttlExpression;
    }

    public void setTtlExpression(Expression expression) {
        this.ttlExpression = expression;
    }

    public boolean isEmpty() {
        return keyExpression == null && ttlExpression == null && !dropTtl && typeExpression == null && !dropStreamMessageId;
    }

}
