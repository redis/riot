package com.redis.riot.cli;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.sync.RediSearchCommands;
import com.redis.lettucemod.search.Field;
import com.redis.lettucemod.search.IndexInfo;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.riot.cli.common.AbstractImportCommand;
import com.redis.riot.cli.common.JobCommandContext;
import com.redis.riot.cli.common.ProgressMonitor;
import com.redis.riot.cli.gen.FakerGeneratorOptions;
import com.redis.riot.core.FakerItemReader;
import com.redis.riot.core.Generator;
import com.redis.riot.core.MapGenerator;
import com.redis.riot.core.MapWithMetadataGenerator;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

@Command(name = "faker", description = "Import from Faker")
public class FakerImport extends AbstractImportCommand {

	private static final String COMMAND_NAME = "import faker";
	private static final Logger log = Logger.getLogger(FakerImport.class.getName());

	@Mixin
	private FakerGeneratorOptions options = new FakerGeneratorOptions();

	public FakerGeneratorOptions getOptions() {
		return options;
	}

	public void setOptions(FakerGeneratorOptions options) {
		this.options = options;
	}

	@Override
	protected Job job(JobCommandContext context) {
		JobBuilder job = context.job(COMMAND_NAME);
		ProgressMonitor monitor = options.configure(progressMonitor()).task("Generating").build();
		return job.start(step(step(context, COMMAND_NAME, reader(context)), monitor).build()).build();
	}

	private AbstractItemCountingItemStreamItemReader<Map<String, Object>> reader(JobCommandContext context) {
		log.log(Level.FINE, "Creating Faker reader with {0}", options);
		FakerItemReader reader = new FakerItemReader(generator(context));
		options.configure(reader);
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
