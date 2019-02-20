package com.redislabs.recharge.redis.set;

import com.redislabs.recharge.redis.CollectionRedisConfiguration;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class SetConfiguration extends CollectionRedisConfiguration {

	private SetCommand command = SetCommand.sadd;

	public static enum SetCommand {
		sadd, srem
	}

}