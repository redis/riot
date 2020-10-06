package com.redislabs.riot.redis;

import org.springframework.batch.item.redis.support.AbstractRedisItemWriter;

import com.redislabs.riot.HelpCommand;

import picocli.CommandLine.Command;

@Command(sortOptions = false, abbreviateSynopsis = true)
public abstract class AbstractRedisCommand<O> extends HelpCommand {

	public abstract AbstractRedisItemWriter<String, String, O> writer();

}
