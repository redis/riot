package com.redislabs.riot.cli;

import org.springframework.batch.item.ItemProcessor;

import com.redislabs.picocliredis.RedisOptions;
import com.redislabs.riot.redis.RedisItemReader;
import com.redislabs.riot.redis.ValueReader;
import com.redislabs.riot.redis.replicate.ScanKeyIterator;

import io.lettuce.core.ScanArgs;
import io.lettuce.core.api.sync.RedisKeyCommands;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(sortOptions = false)
public abstract class ExportCommand<I, O> extends TransferCommand<I, O> {

	@Option(names = "--count", description = "SCAN COUNT option (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private long count = 1000;
	@Option(names = "--match", description = "SCAN MATCH pattern", paramLabel = "<pattern>")
	private String match;
	@Option(names = "--queue", description = "Capacity of the value queue (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private int queue = 10000;
	@Option(names = "--reader-threads", description = "Number of value-reader threads (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private int threads = 1;
	@Option(names = "--reader-batch", description = "Number of values in reader pipeline (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private int pipeline = 50;
	@Option(names = "--reader-timeout", description = "Command timeout duration in seconds (default: ${DEFAULT-VALUE})", paramLabel = "<sec>")
	private int timeout = 2;

	protected int getTimeout() {
		return timeout;
	}

	@Override
	protected ItemProcessor<I, O> processor() throws Exception {
		return null;
	}

	@SuppressWarnings("unchecked")
	protected RedisItemReader<I> reader(ValueReader<I> valueReader) {
		RedisOptions redisOptions = redisOptions();
		ScanArgs args = ScanArgs.Builder.limit(count);
		if (match != null) {
			args.match(match);
		}
		ScanKeyIterator iterator = ScanKeyIterator.builder()
				.commands((RedisKeyCommands<String, String>) redisOptions.redisCommands()).args(args).build();
		return RedisItemReader.builder().keyIterator(iterator).queueCapacity(queue).pool(redisOptions.lettucePool())
				.asyncApi(redisOptions.lettuceAsyncApi()).threads(threads).pipeline(pipeline).reader(valueReader)
				.build();
	}

}