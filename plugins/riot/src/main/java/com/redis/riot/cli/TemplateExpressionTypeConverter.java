package com.redis.riot.cli;

import com.redis.riot.core.SpelUtils;
import com.redis.riot.core.TemplateExpression;

import picocli.CommandLine.ITypeConverter;

public class TemplateExpressionTypeConverter implements ITypeConverter<TemplateExpression> {

    @Override
    public TemplateExpression convert(String value) throws Exception {
        return SpelUtils.parseTemplate(value);
    }

}
