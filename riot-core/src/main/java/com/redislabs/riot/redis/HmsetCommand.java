package com.redislabs.riot.redis;

import java.util.Map;

import org.springframework.batch.item.redis.RedisHashItemWriter;
import org.springframework.vault.support.JsonMapFlattener;

import picocli.CommandLine.Command;

@Command(name = "hmset")
public class HmsetCommand extends AbstractKeyCommand {

	@Override
	public RedisHashItemWriter<String, String, Map<String, Object>> writer() throws Exception {
		return configure(
				RedisHashItemWriter.<Map<String, Object>>builder().mapConverter(JsonMapFlattener::flattenToStringMap))
						.build();
	}

}
