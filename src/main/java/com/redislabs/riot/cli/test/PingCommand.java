package com.redislabs.riot.cli.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redislabs.riot.cli.HelpAwareCommand;
import com.redislabs.riot.cli.redis.RedisDriver;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

@Command(name = "ping", description = "Execute PING command")
public class PingCommand extends HelpAwareCommand {

	private final Logger log = LoggerFactory.getLogger(PingCommand.class);

	@ParentCommand
	private TestConnector test;

	@Override
	public void run() {
		if (test.riot().redis().getDriver() == RedisDriver.jedis) {
			try (JedisPool jedisPool = test.riot().redis().jedisPool()) {
				Jedis jedis = jedisPool.getResource();
				String ping = jedis.ping();
				log.info("Received ping reply: {}", ping);
				jedis.close();
			}
		} else {
			RedisClient client = test.riot().redis().redisClient();
			try (StatefulRedisConnection<String, String> connection = client.connect()) {
				String ping = connection.sync().ping();
				log.info("Received ping reply: {}", ping);
			}
		}

	}

}
