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
import lombok.Setter;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "replicate", description = "Replicate a Redis database")
public class ReplicateCommand extends ImportCommand<KeyValue, KeyValue> implements Runnable {

	@Setter
	@ArgGroup(exclusive = false, heading = "Source Redis connection options%n")
	private RedisOptions sourceRedis = new RedisOptions();
	@Setter
	@ArgGroup(exclusive = false, heading = "Replication options%n")
	private ReplicateOptions options = new ReplicateOptions();

	@Override
	protected KeyValueReader reader() {
		RedisClient client = sourceRedis.lettuceClient();
		ScanArgs args = new ScanArgs().limit(options.count());
		if (options.match() != null) {
			args.match(options.match());
		}
		return valueReader(ScanKeyIterator.builder().connection(client.connect()).scanArgs(args).build());
	}

	private KeyValueReader valueReader(KeyIterator iterator) {
		RedisClient client = sourceRedis.lettuceClient();
		return KeyValueReader.builder().keyIterator(iterator).valueQueueCapacity(options.valueQueueSize())
				.pool(sourceRedis.pool(client::connect)).threads(options.threads()).batchSize(options.batchSize())
				.timeout(options.timeout()).flushRate(options.flushRate()).build();
	}

	@Override
	protected Transfer transfer(ItemReader<KeyValue> reader, ItemProcessor<KeyValue, KeyValue> processor,
			ItemWriter<KeyValue> writer) {
		Transfer transfer = super.transfer(reader, processor, writer);
		if (keyspaceNotificationsEnabled()) {
			KeyspaceNotificationsIterator iterator = KeyspaceNotificationsIterator.builder().channel(options.channel())
					.pubSubConnection(sourceRedis.lettuceClient().connectPubSub()).queueCapacity(options.keyQueueSize())
					.build();
			Flow flow = flow("syncer", valueReader(iterator), processor, writer).flushRate(options.flushRate());
			transfer.flows().add(flow);
		}
		return transfer;
	}

	private boolean keyspaceNotificationsEnabled() {
		return !options.channel().isBlank();
	}

	@Override
	public void run() {
		Restore<Object> restore = new Restore<>();
		restore.setReplace(!options.noReplace());
		execute(restore);
	}

	@Override
	protected String taskName() {
		return "Replicating from " + sourceRedis.getServers().get(0);
	}

}
