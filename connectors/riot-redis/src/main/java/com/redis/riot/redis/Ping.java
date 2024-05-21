package com.redis.riot.redis;

import java.io.PrintWriter;
import java.net.SocketAddress;
import java.text.MessageFormat;
import java.time.Duration;
import java.util.concurrent.Callable;

import org.springframework.util.Assert;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.riot.core.RedisClientOptions;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;

public class Ping implements Callable<Long> {

	public static final Duration DEFAULT_INTERVAL = Duration.ofSeconds(1);
	public static final long DEFAULT_COUNT = Long.MAX_VALUE;

	private static final String REQUEST_MESSAGE = "PING {0}";
	private static final String REPLY_MESSAGE = "{0} from {1}: time={2} ms";

	private PrintWriter out;

	private RedisURI redisURI;
	private RedisClientOptions redisClientOptions = new RedisClientOptions();
	private Duration interval = DEFAULT_INTERVAL;
	private long count = DEFAULT_COUNT;

	public RedisURI getRedisURI() {
		return redisURI;
	}

	public void setRedisURI(RedisURI redisURI) {
		this.redisURI = redisURI;
	}

	public Duration getInterval() {
		return interval;
	}

	public void setInterval(Duration interval) {
		this.interval = interval;
	}

	public void setOut(PrintWriter out) {
		this.out = out;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	public RedisClientOptions getRedisClientOptions() {
		return redisClientOptions;
	}

	public void setRedisClientOptions(RedisClientOptions redisClientOptions) {
		this.redisClientOptions = redisClientOptions;
	}

	@Override
	public Long call() throws Exception {
		Assert.notNull(redisURI, "RedisURI not set");
		out.println(MessageFormat.format(REQUEST_MESSAGE, redisURI));
		try (AbstractRedisClient redisClient = redisClientOptions.redisClient(redisURI);
				StatefulRedisModulesConnection<String, String> connection = RedisModulesUtils.connection(redisClient)) {
			SocketAddress socketAddress = connection.getResources().socketAddressResolver().resolve(redisURI);
			RedisModulesCommands<String, String> redisCommands = connection.sync();
			for (int index = 0; index < count; index++) {
				long startTime = System.nanoTime();
				String reply = redisCommands.ping();
				Assert.isTrue("pong".equalsIgnoreCase(reply), "Invalid PING reply received: " + reply);
				Duration duration = Duration.ofNanos(System.nanoTime() - startTime);
				out.println(MessageFormat.format(REPLY_MESSAGE, reply, socketAddress, duration.toMillis()));
				Thread.sleep(interval.toMillis());
			}
		}
		return count;
	}

}
