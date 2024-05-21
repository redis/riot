package com.redis.riot.cli;

import java.util.Map;

import com.redis.spring.batch.writer.WriteOperation;

public interface WriteOperationCommand {

	WriteOperation<String, String, Map<String, Object>> operation();

}
