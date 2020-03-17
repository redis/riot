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

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

@Command(name = "replicate", description = "Replicate a Redis database to another Redis database")
public class ReplicateCommand extends ExportCommand<KeyDump, KeyDump> {

	@Mixin
	private RedisOptions target = new RedisOptions();
	@ArgGroup(exclusive = false, heading = "Replication options%n")
	private ReplicateOptions replicate = new ReplicateOptions();

	@Override
	protected RedisItemReader<KeyDump> reader() throws Exception {
		return reader(KeyDumpReader.builder().timeout(getReaderOptions().getTimeout()).build());
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Transfer<KeyDump, KeyDump> transfer(ItemReader<KeyDump> reader, ItemProcessor<KeyDump, KeyDump> processor,
			ItemWriter<KeyDump> writer) {
		Transfer<KeyDump, KeyDump> transfer = super.transfer(reader, processor, writer);
		if (replicate.isListener()) {
			RedisOptions source = redisOptions();
			KeyspaceNotificationsIterator iterator = KeyspaceNotificationsIterator.builder()
					.connection(source.statefulRedisPubSubConnection()).channel(replicate.getChannel(source))
					.queueCapacity(replicate.getNotificationQueue()).build();
			KeyDumpReader dumpReader = KeyDumpReader.builder().timeout(replicate.getTimeout()).build();
			RedisItemReader<KeyDump> syncer = RedisItemReader.builder().keyIterator(iterator)
					.queueCapacity(replicate.getQueueSize()).pool(source.lettucePool())
					.asyncApi(source.lettuceAsyncApi()).threads(replicate.getThreads())
					.pipeline(replicate.getPipeline()).reader(dumpReader).flushRate(replicate.getFlushRate()).build();
			Flow<KeyDump, KeyDump> syncerFlow = flow("syncer", syncer, processor, writer);
			syncerFlow.setFlushRate(replicate.getFlushRate());
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
		return writer(target, new Restore<>().replace(!replicate.isNoReplace()));
	}

}
