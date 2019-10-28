package com.redislabs.riot.cli.test;

import java.util.concurrent.TimeUnit;

import com.redislabs.riot.cli.AbstractCommand;
import com.redislabs.riot.cli.redis.RedisConnectionOptions;
import com.redislabs.riot.test.InfoTest;
import com.redislabs.riot.test.LatencyTest;
import com.redislabs.riot.test.PingTest;
import com.redislabs.riot.test.RedisTest;

import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import redis.clients.jedis.Jedis;

@Command(name = "test", description = "Execute a test")
@Slf4j
public class TestCommand extends AbstractCommand {

	@Option(names = { "-t",
			"--test" }, description = "Test to execute: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<name>")
	private RedisTestType test = RedisTestType.ping;
	@Option(names = "--latency-iterations", description = "Number of iterations for latency test (default: ${DEFAULT-VALUE})", paramLabel = "<count>")
	private int latencyIterations = 1000;
	@Option(names = "--latency-sleep", description = "Sleep duration in milliseconds between calls (default: ${DEFAULT-VALUE})", paramLabel = "<ms>")
	private long latencySleep = 1;
	@Option(names = "--latency-unit", description = "Latency unit (default: ${DEFAULT-VALUE})", paramLabel = "<unit>")
	private TimeUnit latencyTimeUnit = TimeUnit.MILLISECONDS;
	@Option(names = "--latency-distribution", description = "Show latency distribution")
	private boolean latencyDistribution;

	@Override
	public void execute(String name, RedisConnectionOptions redis) {
		RedisTest test = test();
		try {
			if (redis.isJedis()) {
				try (Jedis jedis = redis.jedisPool().getResource()) {
					test.execute(jedis);
				}
			} else {
				test.execute(redis.isCluster() ? redis.lettuceClusterClient().connect().sync()
						: redis.lettuceClient().connect().sync());
			}
		} catch (Exception e) {
			log.error("{} was interrupted", name, e);
		}
	}

	private RedisTest test() {
		switch (test) {
		case info:
			return new InfoTest();
		case latency:
			return new LatencyTest(latencyIterations, latencySleep, latencyTimeUnit, latencyDistribution);
		default:
			return new PingTest();
		}

	}

}
