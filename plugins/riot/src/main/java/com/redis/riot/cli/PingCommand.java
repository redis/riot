package com.redis.riot.cli;

import java.time.Duration;
import java.util.concurrent.Callable;

import com.redis.riot.redis.Ping;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

@Command(name = "ping", description = "Test connectivity to a Redis server.", usageHelpAutoWidth = true, abbreviateSynopsis = true)
public class PingCommand implements Callable<Integer> {

	@ParentCommand
	private AbstractMainCommand parent;

	@ArgGroup(exclusive = false, heading = "Redis client options%n")
	private RedisClientArgs redisClientArgs = new RedisClientArgs();

	@Option(names = "--count", description = "Limits the number of ping requests sent to the target.", paramLabel = "<count>")
	private long count = Ping.DEFAULT_COUNT;

	@Option(names = { "-i",
			"--interval" }, description = "Sets the time (in seconds) to wait between sending ping requests (default: ${DEFAULT-VALUE}).", paramLabel = "<sec>")
	private long interval = Ping.DEFAULT_INTERVAL.toSeconds();

	@Override
	public Integer call() throws Exception {
		Ping ping = new Ping();
		ping.setRedisClientOptions(redisClientArgs.redisClientOptions());
		ping.setOut(parent.out);
		ping.setCount(count);
		ping.setInterval(Duration.ofSeconds(interval));
		ping.call();
		return 0;
	}

	public RedisClientArgs getRedisClientArgs() {
		return redisClientArgs;
	}

	public void setRedisClientArgs(RedisClientArgs args) {
		this.redisClientArgs = args;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

}
