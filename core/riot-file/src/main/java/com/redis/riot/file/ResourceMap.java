package com.redis.riot.file;

import org.springframework.core.io.Resource;

public interface ResourceMap {

	String getContentTypeFor(Resource resource);

}
