package com.redislabs.riot.cli.redis;

import org.springframework.batch.item.ItemReader;

import com.redislabs.picocliredis.RedisOptions;
import com.redislabs.riot.batch.redis.KeyValue;
import com.redislabs.riot.batch.redis.writer.Restore;
import com.redislabs.riot.cli.ImportCommand;

import lombok.Setter;
import lombok.experimental.Accessors;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Accessors(fluent = true)
@Command(name = "replicate", description = "Replicate a Redis database")
public class RedisReplicateCommand extends ImportCommand<KeyValue, KeyValue> implements Runnable {

	@Setter
	@ArgGroup(exclusive = false, heading = "Source Redis connection options%n")
	private RedisOptions redisOptions = new RedisOptions();
	@Setter
	@ArgGroup(exclusive = false, heading = "Source Redis options%n")
	private RedisKeyScanOptions keyScanOptions = new RedisKeyScanOptions();

	@SuppressWarnings("unchecked")
	@Override
	protected ItemReader<KeyValue> reader() throws Exception {
		return keyScanOptions.reader(lettuceConnector(redisOptions));
	}

	@Override
	public void run() {
		execute(new Restore<>());
	}

}
