package com.redis.riot.core;

import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.expression.Expression;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import com.redis.lettucemod.util.GeoLocation;

public class EvaluationContextOptions {

    public static final String DEFAULT_DATE_VARIABLE_NAME = "date";

    public static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";

    private Map<String, Object> variables = new LinkedHashMap<>();

    private Map<String, Expression> expressions = new LinkedHashMap<>();

    private String dateVariableName = DEFAULT_DATE_VARIABLE_NAME;

    private String dateFormat = DEFAULT_DATE_FORMAT;

    public String getDateVariableName() {
        return dateVariableName;
    }

    public void setDateVariableName(String name) {
        this.dateVariableName = name;
    }

    public String getDateFormat() {
        return dateFormat;
    }

    public void setDateFormat(String format) {
        this.dateFormat = format;
    }

    public void addVariable(String field, Object value) {
        variables.put(field, value);
    }

    public void addExpression(String field, Expression expression) {
        expressions.put(field, expression);
    }

    public StandardEvaluationContext evaluationContext() {
        StandardEvaluationContext context = new StandardEvaluationContext();
        context.registerFunction("geo", geoMethod());
        Map<String, Object> contextVariables = variables();
        if (!CollectionUtils.isEmpty(contextVariables)) {
            context.setVariables(contextVariables);
        }
        if (!CollectionUtils.isEmpty(expressions)) {
            expressions.forEach((k, v) -> context.setVariable(k, v.getValue(context)));
        }
        return context;
    }

    private Map<String, Object> variables() {
        Map<String, Object> contextVariables = new LinkedHashMap<>(variables);
        if (StringUtils.hasLength(dateVariableName) && StringUtils.hasLength(dateFormat)) {
            contextVariables.put(dateVariableName, new SimpleDateFormat(dateFormat));
        }
        return contextVariables;
    }

    private Method geoMethod() {
        try {
            return GeoLocation.class.getDeclaredMethod("toString", String.class, String.class);
        } catch (Exception e) {
            throw new UnsupportedOperationException("Could not retrieve toString method in GeoLocation class");
        }
    }

}
