package com.redislabs.riot.cli;

import java.util.concurrent.TimeUnit;

import com.redislabs.riot.test.InfoTest;
import com.redislabs.riot.test.LatencyTest;
import com.redislabs.riot.test.PingTest;
import com.redislabs.riot.test.RedisTest;

import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import redis.clients.jedis.Jedis;

@Slf4j
@Command(name = "test", description = "Execute a test", sortOptions = false)
public class TestCommand extends RiotCommand implements Runnable {

	public enum RedisTestType {
		info, ping, latency
	}

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
	public void run() {
		RedisTest test = test();
		try {
			if (redisOptions().isJedis()) {
				try (Jedis jedis = redisOptions().jedisPool().getResource()) {
					test.execute(jedis);
				}
			} else {
				test.execute(redisOptions().redisCommands());
			}
		} catch (Exception e) {
			log.error("Test was interrupted", e);
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
