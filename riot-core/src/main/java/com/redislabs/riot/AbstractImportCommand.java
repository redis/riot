package com.redislabs.riot;

import java.util.ArrayList;
import java.util.List;

import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.util.Assert;

import com.redislabs.riot.redis.EvalCommand;
import com.redislabs.riot.redis.ExpireCommand;
import com.redislabs.riot.redis.GeoaddCommand;
import com.redislabs.riot.redis.HmsetCommand;
import com.redislabs.riot.redis.LpushCommand;
import com.redislabs.riot.redis.NoopCommand;
import com.redislabs.riot.redis.RpushCommand;
import com.redislabs.riot.redis.SaddCommand;
import com.redislabs.riot.redis.SetCommand;
import com.redislabs.riot.redis.XaddCommand;
import com.redislabs.riot.redis.ZaddCommand;

import io.lettuce.core.AbstractRedisClient;
import lombok.Getter;
import picocli.CommandLine.Command;

@Command(subcommands = { EvalCommand.class, ExpireCommand.class, GeoaddCommand.class, HmsetCommand.class,
		LpushCommand.class, NoopCommand.class, RpushCommand.class, SaddCommand.class, SetCommand.class,
		XaddCommand.class,
		ZaddCommand.class }, subcommandsRepeatable = true, synopsisSubcommandLabel = "[REDIS COMMAND]", commandListHeading = "Redis commands:%n")
public abstract class AbstractImportCommand<I, O> extends AbstractTransferCommand<I, O> {

	/*
	 * Initialized manually during command parsing
	 */
	@Getter
	private List<RedisCommand<O>> redisCommands = new ArrayList<>();

	protected ItemWriter<O> writer(AbstractRedisClient client, RedisOptions redisOptions) throws Exception {
		Assert.notNull(redisCommands, "RedisCommands not set");
		List<ItemWriter<O>> writers = new ArrayList<>();
		for (RedisCommand<O> redisCommand : redisCommands) {
			writers.add(redisCommand.writer(client, redisOptions));
		}
		if (writers.isEmpty()) {
			throw new IllegalArgumentException("No Redis command specified");
		}
		if (writers.size() == 1) {
			return (ItemWriter<O>) writers.get(0);
		}
		CompositeItemWriter<O> writer = new CompositeItemWriter<>();
		writer.setDelegates(new ArrayList<>(writers));
		return writer;
	}

	@Override
	protected String transferNameFormat() {
		return "Importing from %s";
	}

}
