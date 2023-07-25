package com.redis.riot.cli;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.batch.core.Job;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.sync.RediSearchCommands;
import com.redis.lettucemod.search.Field;
import com.redis.lettucemod.search.IndexInfo;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.riot.cli.common.AbstractOperationImportCommand;
import com.redis.riot.cli.common.CommandContext;
import com.redis.riot.cli.common.FakerImportOptions;
import com.redis.riot.core.FakerItemReader;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

@Command(name = "faker-import", description = "Import from Faker.")
public class FakerImport extends AbstractOperationImportCommand {

	private static final String TASK = "Generating";

	@Mixin
	private FakerImportOptions fakerImportOptions = new FakerImportOptions();

	public FakerImportOptions getFakerImportOptions() {
		return fakerImportOptions;
	}

	public void setFakerImportOptions(FakerImportOptions options) {
		this.fakerImportOptions = options;
	}

	@Override
	protected Job job(CommandContext context) {
		return job(step(context, reader(context)).task(TASK));
	}

	private FakerItemReader reader(CommandContext context) {
		FakerItemReader reader = new FakerItemReader();
		reader.setMaxItemCount(fakerImportOptions.getCount());
		reader.withIndexRange(fakerImportOptions.getIndexRange());
		fields(context).forEach(reader::withField);
		reader.withLocale(fakerImportOptions.getLocale());
		reader.withIncludeMetadata(fakerImportOptions.isIncludeMetadata());
		return reader;
	}

	private Map<String, String> fields(CommandContext context) {
		Map<String, String> fields = new LinkedHashMap<>();
		fields.putAll(fakerImportOptions.getFields());
		fakerImportOptions.getRedisearchIndex().ifPresent(index -> {
			try (StatefulRedisModulesConnection<String, String> connection = RedisModulesUtils
					.connection(context.getRedisClient())) {
				RediSearchCommands<String, String> commands = connection.sync();
				IndexInfo info = RedisModulesUtils.indexInfo(commands.ftInfo(index));
				for (Field<String> field : info.getFields()) {
					fields.put(field.getName(), expression(field));
				}
			}
		});
		return fields;
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

	@Override
	public String toString() {
		return "FakerImport [fakerImportOptions=" + fakerImportOptions + ", processorOptions=" + processorOptions + ", operationOptions="
				+ operationOptions + ", jobOptions=" + jobOptions + "]";
	}

}
