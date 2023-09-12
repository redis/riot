package com.redis.riot.core;

import java.text.SimpleDateFormat;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.expression.Expression;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.CollectionUtils;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;

public class EvaluationContextOptions {

    public static final String DATE_VARIABLE_NAME = "date";

    public static final String REDIS_VARIABLE_NAME = "redis";

    public static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";

    private String dateFormat = DEFAULT_DATE_FORMAT;

    private Map<String, Expression> expressions = new LinkedHashMap<>();

    private Map<String, Object> variables = new LinkedHashMap<>();

    public String getDateFormat() {
        return dateFormat;
    }

    public void setDateFormat(String format) {
        this.dateFormat = format;
    }

    public Map<String, Expression> getExpressions() {
        return expressions;
    }

    public void setExpressions(Map<String, Expression> expressions) {
        this.expressions = expressions;
    }

    public Map<String, Object> getVariables() {
        return variables;
    }

    public void setVariables(Map<String, Object> variables) {
        this.variables = variables;
    }

    public StandardEvaluationContext evaluationContext(StatefulRedisModulesConnection<String, String> connection) {
        StandardEvaluationContext context = new StandardEvaluationContext();
        context.setVariable(DATE_VARIABLE_NAME, new SimpleDateFormat(dateFormat));
        context.setVariable(REDIS_VARIABLE_NAME, connection.sync());
        if (!CollectionUtils.isEmpty(variables)) {
            variables.forEach(context::setVariable);
        }
        if (!CollectionUtils.isEmpty(expressions)) {
            expressions.forEach((k, v) -> context.setVariable(k, v.getValue(context)));
        }
        return context;
    }

}
