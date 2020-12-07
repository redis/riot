package com.redislabs.riot.redis;

import java.util.Map;

import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.HashItemWriter;

import com.redislabs.riot.TransferContext;
import com.redislabs.riot.convert.MapFlattener;
import com.redislabs.riot.convert.ObjectToStringConverter;

import picocli.CommandLine.Command;

@Command(name = "hmset", aliases = "h", description = "Set hashes from input")
public class HmsetCommand extends AbstractKeyCommand {

	@Override
	public ItemWriter<Map<String, Object>> writer(TransferContext context) throws Exception {
		return configure(HashItemWriter.<Map<String, Object>>builder(context.getClient())
				.poolConfig(context.getRedisOptions().poolConfig())
				.mapConverter(new MapFlattener<>(new ObjectToStringConverter()))).build();
	}

}
