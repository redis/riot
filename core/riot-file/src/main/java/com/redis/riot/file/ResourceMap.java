package com.redis.riot.file;

import org.springframework.core.io.Resource;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;

public interface ResourceMap {

	MimeType CSV = new MimeType("text", "csv");
	MimeType PSV = new MimeType("text", "psv");
	MimeType TSV = new MimeType("text", "tsv");
	MimeType TEXT = new MimeType("text", "plain");
	MimeType JSON = MimeTypeUtils.APPLICATION_JSON;
	MimeType JSON_LINES = new MimeType("application", "jsonlines");
	MimeType XML = MimeTypeUtils.APPLICATION_XML;

	MimeType getContentTypeFor(Resource resource);

}
