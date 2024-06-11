package com.redis.riot;

import org.springframework.batch.core.Job;

import com.redis.riot.core.Step;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.gen.GeneratorItemReader;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "generate", description = "Generate Redis data structures.")
public class Generate extends AbstractRedisArgsCommand {

	private static final String TASK_NAME = "Generating";

	@ArgGroup(exclusive = false)
	private GenerateArgs generatorArgs = new GenerateArgs();

	@ArgGroup(exclusive = false, heading = "Redis writer options%n")
	private RedisWriterArgs redisWriterArgs = new RedisWriterArgs();

	@Override
	protected Job job() {
		return job(new Step<>(reader(), writer()).taskName(TASK_NAME).maxItemCount(generatorArgs.getCount()));
	}

	private GeneratorItemReader reader() {
		GeneratorItemReader reader = new GeneratorItemReader();
		reader.setMaxItemCount(generatorArgs.getCount());
		reader.setOptions(generatorArgs.generatorOptions());
		return reader;
	}

	private RedisItemWriter<String, String, KeyValue<String, Object>> writer() {
		RedisItemWriter<String, String, KeyValue<String, Object>> writer = RedisItemWriter.struct();
		writer.setClient(client.getClient());
		redisWriterArgs.configure(writer);
		return writer;
	}

	public RedisWriterArgs getRedisWriterArgs() {
		return redisWriterArgs;
	}

	public void setRedisWriterArgs(RedisWriterArgs args) {
		this.redisWriterArgs = args;
	}

}
