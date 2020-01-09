package com.redislabs.riot.cli.redis;

import java.util.Arrays;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;

import com.redislabs.picocliredis.RedisOptions;
import com.redislabs.riot.cli.ImportCommand;
import com.redislabs.riot.redis.KeyValue;
import com.redislabs.riot.redis.replicate.LettuceKeyScanReader;
import com.redislabs.riot.redis.writer.Restore;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Slf4j
@Command(name = "replicate", description = "Replicate a Redis database")
public class ReplicateCommand extends ImportCommand<KeyValue, KeyValue> implements Runnable {

	@Setter
	@ArgGroup(exclusive = false, heading = "Source Redis connection options%n")
	private RedisOptions sourceRedis = new RedisOptions();
	@Setter
	@ArgGroup(exclusive = false, heading = "Replication options%n")
	private ReplicateOptions options = new ReplicateOptions();

	@Override
	protected LettuceKeyScanReader reader() {
		RedisClient client = sourceRedis.lettuceClient();
		return new LettuceKeyScanReader().limit(options.count()).match(options.match()).connection(client.connect())
				.queueCapacity(options.queueSize()).pool(sourceRedis.pool(client::connect)).threads(options.threads())
				.batchSize(options.batchSize()).timeout(options.timeout());
	}

	@Override
	protected void execute(ItemReader<KeyValue> reader, ItemProcessor<KeyValue, KeyValue> processor,
			ItemWriter<KeyValue> writer) {
		StatefulRedisConnection<String, String> connection = sourceRedis.lettuceClient().connect();
		if (!options.channel().isBlank()) {
			StatefulRedisPubSubConnection<String, String> pubSub = sourceRedis.lettuceClient().connectPubSub();
			pubSub.addListener(new RedisPubSubAdapter<String, String>() {
				@Override
				public void message(String pattern, String channel, String message) {
					String key = channel.substring(channel.indexOf(":") + 1);
					Long ttl = connection.sync().pttl(key);
					byte[] value = connection.sync().dump(key);
					KeyValue keyValue = new KeyValue(key, ttl, value);
					try {
						writer.write(Arrays.asList(keyValue));
					} catch (Exception e) {
						log.error("Could not write key '{}'", key, e);
					}
				}
			});
			pubSub.sync().psubscribe(options.channel());
		}
		super.execute(reader, processor, writer);
	}

	@Override
	protected void close(ItemReader<KeyValue> reader, ItemWriter<KeyValue> writer) {
		if (!options.channel().isBlank() && options.listen()) {
			while (true) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					log.debug("Interrupted while sleeping for keyspace notifications", e);
				}
			}
		}
		super.close(reader, writer);
	}

	@Override
	public void run() {
		Restore<Object> restore = new Restore<>();
		restore.setReplace(options.replace());
		execute(restore);
	}

	@Override
	protected String taskName() {
		return "Replicating from " + sourceRedis.getServers().get(0);
	}

}
