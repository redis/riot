package com.redis.riot.core.operation;

import java.util.Map;
import java.util.function.Function;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redis.riot.core.function.ObjectMapperFunction;
import com.redis.spring.batch.writer.operation.Set;

public class SetBuilder extends AbstractOperationBuilder<SetBuilder> {

    public enum StringFormat {
        RAW, XML, JSON
    }

    public static final StringFormat DEFAULT_FORMAT = StringFormat.JSON;

    private StringFormat format = DEFAULT_FORMAT;

    private String field;

    private String root;

    public SetBuilder format(StringFormat format) {
        this.format = format;
        return this;
    }

    public SetBuilder field(String field) {
        this.field = field;
        return this;
    }

    public SetBuilder root(String root) {
        this.root = root;
        return this;
    }

    @Override
    protected Set<String, String, Map<String, Object>> operation() {
        return new Set<String, String, Map<String, Object>>().value(value());
    }

    private Function<Map<String, Object>, String> value() {
        switch (format) {
            case RAW:
                if (field == null) {
                    throw new IllegalArgumentException("Raw value field name not set");
                }
                return toString(field);
            case XML:
                return new ObjectMapperFunction<>(new XmlMapper().writer().withRootName(root));
            default:
                ObjectMapper jsonMapper = new ObjectMapper();
                jsonMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
                jsonMapper.setSerializationInclusion(Include.NON_NULL);
                return new ObjectMapperFunction<>(jsonMapper.writer().withRootName(root));
        }
    }

}
