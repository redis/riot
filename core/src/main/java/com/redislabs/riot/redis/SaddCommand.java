package com.redislabs.riot.redis;

import java.util.Map;

import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.SetItemWriter;

import com.redislabs.riot.TransferContext;

import picocli.CommandLine.Command;

@Command(name = "sadd", aliases = "a", description = "Add members to sets")
public class SaddCommand extends AbstractCollectionCommand {

	@Override
	public ItemWriter<Map<String, Object>> writer(TransferContext context) throws Exception {
		return configure(SetItemWriter.<Map<String, Object>>builder(context.getClient())
				.poolConfig(context.getRedisOptions().poolConfig())).build();
	}

}
