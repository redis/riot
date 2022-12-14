package com.redis.riot.gen;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.sync.RediSearchCommands;
import com.redis.lettucemod.search.Field;
import com.redis.lettucemod.search.IndexInfo;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.riot.AbstractImportCommand;
import com.redis.riot.JobCommandContext;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

@Command(name = "faker", description = "Import Faker data using the Spring Expression Language (SpEL)")
public class FakerGeneratorCommand extends AbstractImportCommand {

	private static final Logger log = Logger.getLogger(FakerGeneratorCommand.class.getName());

	private static final String NAME = "faker-import";

	@Mixin
	private FakerGeneratorOptions options = new FakerGeneratorOptions();

	public FakerGeneratorOptions getOptions() {
		return options;
	}

	public void setOptions(FakerGeneratorOptions options) {
		this.options = options;
	}

	@Override
	protected Job job(JobCommandContext context) throws Exception {
		SimpleStepBuilder<Map<String, Object>, Map<String, Object>> step = step(context, NAME, reader(context));
		return job(context, NAME, step, options.configure(progressMonitor()).task("Generating").build());
	}

	private AbstractItemCountingItemStreamItemReader<Map<String, Object>> reader(JobCommandContext context) {
		log.log(Level.FINE, "Creating Faker reader with {0}", options);
		FakerItemReader reader = new FakerItemReader(generator(context));
		reader.setStart(options.getStart());
		reader.setCount(options.getCount());
		return reader;
	}

	private void addFieldsFromIndex(JobCommandContext context, String index, Map<String, String> fields) {
		try (StatefulRedisModulesConnection<String, String> connection = context.connection()) {
			RediSearchCommands<String, String> commands = connection.sync();
			IndexInfo info = RedisModulesUtils.indexInfo(commands.ftInfo(index));
			for (Field<String> field : info.getFields()) {
				fields.put(field.getName(), expression(field));
			}
		}
	}

	private Generator<Map<String, Object>> generator(JobCommandContext context) {
		Map<String, String> fields = new LinkedHashMap<>(options.getFields());
		options.getRedisearchIndex().ifPresent(index -> addFieldsFromIndex(context, index, fields));
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

}
