package com.redis.riot.core;

import org.springframework.expression.Expression;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import com.redis.riot.core.function.ExpressionFunction;
import com.redis.riot.core.function.KeyValueOperator;
import com.redis.riot.core.function.LongExpressionFunction;

import io.lettuce.core.codec.RedisCodec;

public class KeyValueProcessorOptions {

    private TemplateExpression keyExpression;

    private Expression typeExpression;

    private Expression ttlExpression;

    private EvaluationContextOptions evaluationContextOptions = new EvaluationContextOptions();

    public TemplateExpression getKeyExpression() {
        return keyExpression;
    }

    public void setKeyExpression(TemplateExpression expression) {
        this.keyExpression = expression;
    }

    public Expression getTypeExpression() {
        return typeExpression;
    }

    public void setTypeExpression(Expression expression) {
        this.typeExpression = expression;
    }

    public Expression getTtlExpression() {
        return ttlExpression;
    }

    public void setTtlExpression(Expression expression) {
        this.ttlExpression = expression;
    }

    public boolean isEmpty() {
        return keyExpression == null && typeExpression == null && ttlExpression == null;
    }

    public EvaluationContextOptions getEvaluationContextOptions() {
        return evaluationContextOptions;
    }

    public void setEvaluationContextOptions(EvaluationContextOptions evaluationContextOptions) {
        this.evaluationContextOptions = evaluationContextOptions;
    }

    public <K> KeyValueOperator<K> operator(RedisCodec<K, ?> codec) {
        if (isEmpty()) {
            return null;
        }
        StandardEvaluationContext context = evaluationContextOptions.evaluationContext();
        KeyValueOperator<K> operator = new KeyValueOperator<>(codec);
        if (keyExpression != null) {
            operator.key(ExpressionFunction.of(context, keyExpression));
        }
        if (typeExpression != null) {
            operator.type(ExpressionFunction.of(context, typeExpression, String.class));
        }
        if (ttlExpression != null) {
            operator.ttl(new LongExpressionFunction<>(context, ttlExpression));
        }
        return operator;
    }

}
