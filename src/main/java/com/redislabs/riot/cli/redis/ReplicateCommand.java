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

import io.lettuce.core.RedisClient;
import io.lettuce.core.ScanArgs;
import lombok.Data;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "replicate", description = "Replicate a Redis database")
public @Data class ReplicateCommand extends ImportCommand<KeyValue, KeyValue> implements Runnable {

	@ArgGroup(exclusive = false, heading = "Source Redis connection options%n")
	private RedisOptions sourceRedis = new RedisOptions();
	@ArgGroup(exclusive = false, heading = "Replication options%n")
	private ReplicateOptions options = new ReplicateOptions();

	@Override
	protected KeyValueReader reader() {
		RedisClient client = sourceRedis.lettuceClient();
		ScanArgs args = new ScanArgs().limit(options.count());
		if (options.match() != null) {
			args.match(options.match());
		}
		return valueReader(new ScanKeyIterator().connection(client.connect()).args(args));
	}

	private KeyValueReader valueReader(KeyIterator iterator) {
		RedisClient client = sourceRedis.lettuceClient();
		return new KeyValueReader().keyIterator(iterator).valueQueueCapacity(options.valueQueueSize())
				.pool(sourceRedis.pool(client::connect)).threads(options.threads()).batchSize(options.batchSize())
				.timeout(options.timeout()).flushRate(options.flushRate());
	}

	@Override
	protected Transfer transfer(ItemReader<KeyValue> reader, ItemProcessor<KeyValue, KeyValue> processor,
			ItemWriter<KeyValue> writer) {
		Transfer transfer = super.transfer(reader, processor, writer);
		if (keyspaceNotificationsEnabled()) {
			KeyspaceNotificationsIterator iterator = new KeyspaceNotificationsIterator(
					sourceRedis.lettuceClient().connectPubSub(), options.channel(), options.keyQueueSize());
			Flow flow = flow("syncer", valueReader(iterator), processor, writer).flushRate(options.flushRate());
			transfer.flow(flow);
		}
		return transfer;
	}

	private boolean keyspaceNotificationsEnabled() {
		return !options.channel().isBlank();
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
