package com.redislabs.riot;

import io.lettuce.core.RedisURI;
import picocli.CommandLine;

public class RedisURIConverter implements CommandLine.ITypeConverter<RedisURI> {

    @Override
    public RedisURI convert(String value) {
        try {
            return RedisURI.create(value);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid Redis connection string", e);
        }
    }

}
