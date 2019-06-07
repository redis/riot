
package com.redislabs.riot.cli;

import com.redislabs.riot.RiotApplication.RedisDriver;
import com.redislabs.riot.redis.writer.JedisCommands;
import com.redislabs.riot.redis.writer.LettuceCommands;
import com.redislabs.riot.redis.writer.RedisCommands;

import lombok.Setter;

public class CommandsBuilder {

	@Setter
	private RedisDriver driver;

	protected RedisCommands build() {
		switch (driver) {
		case Lettuce:
			return lettuceCommands();
		default:
			return jedisCommands();
		}
	}

	protected RedisCommands jedisCommands() {
		return new JedisCommands();
	}

	protected RedisCommands lettuceCommands() {
		return new LettuceCommands();
	}

}
