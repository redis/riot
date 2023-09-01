package com.redis.riot.cli;

import org.springframework.expression.Expression;

import com.redis.riot.core.SpelUtils;

import picocli.CommandLine.ITypeConverter;

public class ExpressionTypeConverter implements ITypeConverter<Expression> {

    @Override
    public Expression convert(String value) throws Exception {
        return SpelUtils.parse(value);
    }

}
