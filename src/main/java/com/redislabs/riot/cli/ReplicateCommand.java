package com.redislabs.riot.cli;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;

import com.redislabs.picocliredis.RedisOptions;
import com.redislabs.riot.redis.KeyDump;
import com.redislabs.riot.redis.RedisItemReader;
import com.redislabs.riot.redis.replicate.KeyDumpReader;
import com.redislabs.riot.redis.replicate.KeyspaceNotificationsIterator;
import com.redislabs.riot.redis.writer.map.Restore;
import com.redislabs.riot.transfer.Flow;
import com.redislabs.riot.transfer.Transfer;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

@Command(name = "replicate", description = "Replicate a Redis database to another Redis database", sortOptions = false)
public class ReplicateCommand extends ExportCommand<KeyDump, KeyDump> {

	private final static String DATABASE_TOKEN = "{database}";

	@Mixin
	private RedisOptions target = new RedisOptions();
	@Option(names = "--notification-queue", description = "Capacity of the keyspace notification queue (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private int notificationQueue = 10000;
	@Option(names = "--keyspace-channel", description = "Pub/sub channel for keyspace events (default: ${DEFAULT-VALUE}). Blank to disable", paramLabel = "<string>")
	private String channel = "__keyspace@" + DATABASE_TOKEN + "__:*";
	@Option(names = "--no-replace", description = "No REPLACE modifier with RESTORE command")
	private boolean noReplace;
	@Option(names = "--flush-rate", description = "Interval in millis between notification flushes (default: ${DEFAULT-VALUE})", paramLabel = "<ms>")
	private long flushRate = 50;
	@Option(names = "--syncer-timeout", description = "Syncer timeout duration in seconds (default: ${DEFAULT-VALUE})", paramLabel = "<sec>")
	private int timeout = 2;
	@Option(names = "--syncer-pipeline", description = "Number of values in dump pipeline (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private int pipeline = 50;
	@Option(names = "--syncer-queue", description = "Capacity of the value queue (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private int queueSize = 10000;
	@Option(names = "--syncer-threads", description = "Number of value reader threads (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private int threads = 1;
	@Option(names = "--listener", description = "Enable keyspace notification listener")
	private boolean listener;

	public String getChannel(RedisOptions redisOptions) {
		return channel.replace(DATABASE_TOKEN, String.valueOf(redisOptions.getDatabase()));
	}

	@Override
	protected RedisItemReader<KeyDump> reader() throws Exception {
		return reader(KeyDumpReader.builder().timeout(getTimeout()).build());
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Transfer<KeyDump, KeyDump> transfer(ItemReader<KeyDump> reader, ItemProcessor<KeyDump, KeyDump> processor,
			ItemWriter<KeyDump> writer) {
		Transfer<KeyDump, KeyDump> transfer = super.transfer(reader, processor, writer);
		if (listener) {
			RedisOptions source = redisOptions();
			KeyspaceNotificationsIterator iterator = KeyspaceNotificationsIterator.builder()
					.connection(source.statefulRedisPubSubConnection()).channel(getChannel(source))
					.queueCapacity(notificationQueue).build();
			KeyDumpReader dumpReader = KeyDumpReader.builder().timeout(timeout).build();
			RedisItemReader<KeyDump> syncer = RedisItemReader.builder().keyIterator(iterator).queueCapacity(queueSize)
					.pool(source.lettucePool()).asyncApi(source.lettuceAsyncApi()).threads(threads).pipeline(pipeline)
					.reader(dumpReader).flushRate(flushRate).build();
			Flow<KeyDump, KeyDump> syncerFlow = flow("syncer", syncer, processor, writer);
			syncerFlow.setFlushRate(flushRate);
			transfer.add(syncerFlow);
		}
		return transfer;
	}

	@Override
	protected String taskName() {
		return "Replicating " + redisOptions().getServers().get(0) + " to " + target.getServers().get(0);
	}

	@Override
	protected ItemWriter<KeyDump> writer() throws Exception {
		return writer(target, new Restore<>().replace(!noReplace));
	}

}
