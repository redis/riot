package com.redis.riot;

import io.lettuce.core.RedisURI;
import picocli.CommandLine.ITypeConverter;

public class RedisURIConverter implements ITypeConverter<RedisURI> {

	@Override
	public RedisURI convert(String value) {
		try {
			return RedisURI.create(value);
		} catch (IllegalArgumentException e) {
			return RedisURI.create(RedisURI.URI_SCHEME_REDIS + "://" + value);
		}
	}

}
