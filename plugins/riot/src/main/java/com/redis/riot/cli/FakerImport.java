package com.redis.riot.cli;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.sync.RediSearchCommands;
import com.redis.lettucemod.search.Field;
import com.redis.lettucemod.search.IndexInfo;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.riot.cli.common.AbstractImportCommand;
import com.redis.riot.cli.common.CommandContext;
import com.redis.riot.cli.common.FakerImportOptions;
import com.redis.riot.cli.common.StepProgressMonitor;
import com.redis.riot.core.FakerItemReader;
import com.redis.riot.core.Generator;
import com.redis.riot.core.MapGenerator;
import com.redis.riot.core.MapWithMetadataGenerator;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

@Command(name = "faker-import", description = "Import from Faker.")
public class FakerImport extends AbstractImportCommand {

	private static final Logger log = Logger.getLogger(FakerImport.class.getName());

	@Mixin
	private FakerImportOptions options = new FakerImportOptions();

	public FakerImportOptions getOptions() {
		return options;
	}

	public void setOptions(FakerImportOptions options) {
		this.options = options;
	}

	@Override
	protected Job job(CommandContext context) {
		log.log(Level.FINE, "Creating Faker reader with {0}", options);
		FakerItemReader reader = new FakerItemReader(generator(context));
		reader.setCurrentItemCount(options.getStart() - 1);
		reader.setMaxItemCount(options.getCount());
		SimpleStepBuilder<Map<String, Object>, Map<String, Object>> step = step(context.getRedisClient(), reader);
		StepProgressMonitor monitor = progressMonitor("Generating");
		monitor.withInitialMax(options.getCount());
		monitor.register(step);
		return job(commandName()).start(step.build()).build();
	}

	private void addFieldsFromIndex(CommandContext context, String index, Map<String, String> fields) {
		try (StatefulRedisModulesConnection<String, String> connection = RedisModulesUtils
				.connection(context.getRedisClient())) {
			RediSearchCommands<String, String> commands = connection.sync();
			IndexInfo info = RedisModulesUtils.indexInfo(commands.ftInfo(index));
			for (Field<String> field : info.getFields()) {
				fields.put(field.getName(), expression(field));
			}
		}
	}

	private Generator<Map<String, Object>> generator(CommandContext context) {
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
