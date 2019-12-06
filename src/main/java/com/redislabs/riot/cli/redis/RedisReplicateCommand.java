package com.redislabs.riot.cli.redis;

import org.springframework.batch.item.ItemReader;

import com.redislabs.picocliredis.RedisOptions;
import com.redislabs.riot.batch.TransferContext;
import com.redislabs.riot.batch.redis.KeyValue;
import com.redislabs.riot.batch.redis.reader.LettuceKeyScanReader;
import com.redislabs.riot.batch.redis.writer.Restore;
import com.redislabs.riot.cli.ImportCommand;

import io.lettuce.core.RedisClient;
import lombok.Setter;
import lombok.experimental.Accessors;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Accessors(fluent = true)
@Command(name = "replicate", description = "Replicate a Redis database")
public class RedisReplicateCommand extends ImportCommand<KeyValue, KeyValue> implements Runnable {

	@Setter
	@ArgGroup(exclusive = false, heading = "Source Redis connection options%n")
	private RedisOptions redis = new RedisOptions();
	@Setter
	@ArgGroup(exclusive = false, heading = "Source Redis options%n")
	private RedisKeyScanOptions keyScan = new RedisKeyScanOptions();

	@Override
	protected ItemReader<KeyValue> reader(TransferContext context) throws Exception {
		RedisClient client = redis.lettuceClient();
		return new LettuceKeyScanReader().connection(client.connect()).pool(redis.pool(client::connect))
				.count(keyScan.count()).match(keyScan.match());
	}

	@Override
	public void run() {
		execute("restore", new Restore<>());
	}

	@Override
	protected String taskName() {
		return "Replicating from " + redis.servers().get(0);
	}

}
