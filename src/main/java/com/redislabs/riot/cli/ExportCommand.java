package com.redislabs.riot.cli;

import com.redislabs.picocliredis.RedisOptions;
import com.redislabs.riot.redis.RedisItemReader;
import com.redislabs.riot.redis.ValueReader;
import com.redislabs.riot.redis.replicate.ScanKeyIterator;

import io.lettuce.core.ScanArgs;
import io.lettuce.core.api.sync.RedisKeyCommands;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command
public abstract class ExportCommand<I, O> extends TransferCommand<I, O> {

	@ArgGroup(exclusive = false, heading = "Redis export options%n")
	private ExportOptions options = new ExportOptions();

	protected ExportOptions getReaderOptions() {
		return options;
	}

	@SuppressWarnings("unchecked")
	protected RedisItemReader<I> reader(ValueReader<I> valueReader) {
		RedisOptions redisOptions = redisOptions();
		ScanArgs args = ScanArgs.Builder.limit(options.getCount());
		if (options.getMatch() != null) {
			args.match(options.getMatch());
		}
		ScanKeyIterator iterator = ScanKeyIterator.builder()
				.commands((RedisKeyCommands<String, String>) redisOptions.redisCommands()).args(args).build();
		return RedisItemReader.builder().keyIterator(iterator).queueCapacity(options.getQueue())
				.pool(redisOptions.lettucePool()).asyncApi(redisOptions.lettuceAsyncApi()).threads(options.getThreads())
				.pipeline(options.getPipeline()).reader(valueReader).build();
	}

}