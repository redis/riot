package com.redislabs.riot.cli.redis;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;

import com.redislabs.picocliredis.RedisOptions;
import com.redislabs.riot.cli.ImportCommand;
import com.redislabs.riot.redis.KeyValue;
import com.redislabs.riot.redis.replicate.KeyIterator;
import com.redislabs.riot.redis.replicate.KeyValueReader;
import com.redislabs.riot.redis.replicate.KeyspaceNotificationsIterator;
import com.redislabs.riot.redis.replicate.ScanKeyIterator;
import com.redislabs.riot.redis.writer.Restore;
import com.redislabs.riot.transfer.Flow;
import com.redislabs.riot.transfer.Transfer;

import io.lettuce.core.ScanArgs;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.sync.RedisKeyCommands;
import io.lettuce.core.api.sync.RedisServerCommands;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Slf4j
@Command(name = "replicate", description = "Replicate a Redis database")
public @Data class ReplicateCommand extends ImportCommand<KeyValue, KeyValue> implements Runnable {

	@ArgGroup(exclusive = false, heading = "Source Redis connection options%n")
	private RedisOptions sourceRedis = new RedisOptions();
	@ArgGroup(exclusive = false, heading = "Replication options%n")
	private ReplicateOptions options = new ReplicateOptions();

	@SuppressWarnings("unchecked")
	@Override
	protected KeyValueReader reader() {
		ScanArgs args = new ScanArgs().limit(options.count());
		if (options.match() != null) {
			args.match(options.match());
		}
		RedisServerCommands<String, String> serverCommands = (RedisServerCommands<String, String>) sourceRedis
				.redisCommands();
		log.debug("Source Redis size: {}", serverCommands.dbsize());
		return valueReader(ScanKeyIterator.<StatefulConnection<String, String>>builder()
				.commands((RedisKeyCommands<String, String>) sourceRedis.redisCommands()).args(args).build());
	}

	@SuppressWarnings("unchecked")
	private KeyValueReader valueReader(KeyIterator iterator) {
		return KeyValueReader.builder().keyIterator(iterator).valueQueueCapacity(options.valueQueueSize())
				.pool(sourceRedis.lettucePool()).asyncApi(sourceRedis.lettuceAsyncApi()).threads(options.threads())
				.batchSize(options.batchSize()).timeout(options.timeout()).flushRate(options.flushRate()).build();
	}

	@Override
	protected Transfer transfer(ItemReader<KeyValue> reader, ItemProcessor<KeyValue, KeyValue> processor,
			ItemWriter<KeyValue> writer) {
		Transfer transfer = super.transfer(reader, processor, writer);
		if (keyspaceNotificationsEnabled()) {
			KeyspaceNotificationsIterator iterator = KeyspaceNotificationsIterator.builder()
					.connection(sourceRedis.statefulRedisPubSubConnection()).channel(options.channel())
					.queueCapacity(options.keyQueueSize()).build();
			KeyValueReader valueReader = valueReader(iterator);
			Flow flow = flow("syncer", valueReader, processor, writer).flushRate(options.flushRate());
			transfer.flow(flow);
		}
		return transfer;
	}

	private boolean keyspaceNotificationsEnabled() {
		return options.channel().length() > 0;
	}

	@Override
	public void run() {
		Restore<Object> restore = new Restore<>();
		restore.replace(!options.noReplace());
		execute(restore);
	}

	@Override
	protected String taskName() {
		return "Replicating";
	}

}
