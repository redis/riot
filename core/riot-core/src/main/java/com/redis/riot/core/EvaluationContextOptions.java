package com.redis.riot.core;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.expression.Expression;

public class EvaluationContextOptions {

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

}
