package com.redis.riot;

import java.util.ArrayList;
import java.util.List;

import org.springframework.batch.core.Job;
import org.springframework.util.StringUtils;

import com.redis.lettucemod.search.CreateOptions;
import com.redis.lettucemod.search.Field;
import com.redis.riot.core.Step;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.gen.GeneratorItemReader;
import com.redis.spring.batch.item.redis.gen.ItemType;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "gen", aliases = "generate", description = "Generate Redis data structures.")
public class Generate extends AbstractRedisCommand {

	private static final String TASK_NAME = "Generating";

	@ArgGroup(exclusive = false)
	private GenerateArgs generateArgs = new GenerateArgs();

	@ArgGroup(exclusive = false, heading = "Redis writer options%n")
	private RedisWriterArgs redisWriterArgs = new RedisWriterArgs();

	@Override
	protected Job job() {
		if (StringUtils.hasLength(generateArgs.getIndex())) {
			commands().ftCreate(generateArgs.getIndex(), indexCreateOptions(), indexFields());
		}
		Step<KeyValue<String>, KeyValue<String>> step = new Step<>(reader(), writer());
		step.taskName(TASK_NAME);
		step.maxItemCount(generateArgs.getCount());
		return job(step);
	}

	private RedisItemWriter<String, String, KeyValue<String>> writer() {
		RedisItemWriter<String, String, KeyValue<String>> writer = RedisItemWriter.struct();
		configure(writer);
		log.info("Configuring Redis writer with {}", redisWriterArgs);
		redisWriterArgs.configure(writer);
		return writer;
	}

	private CreateOptions<String, String> indexCreateOptions() {
		CreateOptions.Builder<String, String> options = CreateOptions.builder();
		options.on(indexOn());
		options.prefix(generateArgs.getKeyspace() + generateArgs.getKeySepataror());
		return options.build();
	}

	private CreateOptions.DataType indexOn() {
		if (isJson()) {
			return CreateOptions.DataType.JSON;
		}
		return CreateOptions.DataType.HASH;
	}

	private boolean isJson() {
		return generateArgs.getTypes().contains(ItemType.JSON);
	}

	@SuppressWarnings("unchecked")
	private Field<String>[] indexFields() {
		int fieldCount = indexFieldCount();
		List<Field<String>> fields = new ArrayList<>();
		for (int index = 1; index <= fieldCount; index++) {
			fields.add(indexField(index));
		}
		return fields.toArray(new Field[0]);
	}

	private Field<String> indexField(int index) {
		String name = "field" + index;
		if (isJson()) {
			return Field.tag("$." + name).as(name).build();
		}
		return Field.tag(name).build();
	}

	private int indexFieldCount() {
		if (isJson()) {
			return generateArgs.getJsonFieldCount().getMax();
		}
		return generateArgs.getHashFieldCount().getMax();
	}

	private GeneratorItemReader reader() {
		GeneratorItemReader reader = new GeneratorItemReader();
		reader.setMaxItemCount(generateArgs.getCount());
		generateArgs.configure(reader);
		return reader;
	}

	public RedisWriterArgs getRedisWriterArgs() {
		return redisWriterArgs;
	}

	public void setRedisWriterArgs(RedisWriterArgs args) {
		this.redisWriterArgs = args;
	}

	public GenerateArgs getGenerateArgs() {
		return generateArgs;
	}

	public void setGenerateArgs(GenerateArgs args) {
		this.generateArgs = args;
	}

}
