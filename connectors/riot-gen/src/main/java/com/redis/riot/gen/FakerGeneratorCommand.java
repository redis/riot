package com.redis.riot.gen;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemReader;

import com.redis.lettucemod.RedisModulesUtils;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.sync.RediSearchCommands;
import com.redis.lettucemod.search.Field;
import com.redis.lettucemod.search.IndexInfo;
import com.redis.riot.AbstractImportCommand;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "faker", description = "Import Faker data using the Spring Expression Language (SpEL)")
public class FakerGeneratorCommand extends AbstractImportCommand {

	private static final Logger log = LoggerFactory.getLogger(FakerGeneratorCommand.class);

	private static final String NAME = "faker-import";

	@CommandLine.Mixin
	private FakerGeneratorOptions options = new FakerGeneratorOptions();

	@Override
	protected Job job(JobBuilder jobBuilder) throws Exception {
		TaskletStep step = step(NAME, "Generating", reader()).build();
		return jobBuilder.start(step).build();
	}

	@Override
	protected Long initialMax() {
		return (long) options.getEnd() - options.getStart();
	}

	private ItemReader<Map<String, Object>> reader() {
		log.debug("Creating Faker reader with {}", options);
		FakerItemReader reader = new FakerItemReader(generator());
		reader.setStart(options.getStart());
		reader.setEnd(options.getEnd());
		if (options.getSleep() > 0) {
			return new ThrottledItemReader<>(reader, options.getSleep());
		}
		return reader;
	}

	private Generator<Map<String, Object>> generator() {
		Map<String, String> fakerFields = options.getFields();
		Map<String, String> fields = fakerFields == null ? new LinkedHashMap<>() : new LinkedHashMap<>(fakerFields);
		Optional<String> fakerIndex = options.getRedisearchIndex();
		fakerIndex.ifPresent(i -> fields.putAll(fieldsFromIndex(i)));
		MapGenerator generator = MapGenerator.builder().locale(options.getLocale()).fields(fields).build();
		if (options.isIncludeMetadata()) {
			return new MapWithMetadataGenerator(generator);
		}
		return generator;
	}

	private String expression(Field<String> field) {
		switch (field.getType()) {
		case TEXT:
			return "lorem.paragraph";
		case TAG:
			return "number.digits(10)";
		case GEO:
			return "address.longitude.concat(',').concat(address.latitude)";
		default:
			return "number.randomDouble(3,-1000,1000)";
		}
	}

	private Map<String, String> fieldsFromIndex(String index) {
		Map<String, String> fields = new LinkedHashMap<>();
		try (StatefulRedisModulesConnection<String, String> connection = getRedisOptions().connect()) {
			RediSearchCommands<String, String> commands = connection.sync();
			IndexInfo info = RedisModulesUtils.indexInfo(commands.ftInfo(index));
			for (Field<String> field : info.getFields()) {
				fields.put(field.getName(), expression(field));
			}
		}
		return fields;
	}

}
