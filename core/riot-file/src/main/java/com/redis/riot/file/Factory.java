package com.redis.riot.file;

import org.springframework.core.io.Resource;

public interface Factory<T, O> {

	T create(Resource resource, O options);

}
