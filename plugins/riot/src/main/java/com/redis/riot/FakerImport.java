package com.redis.riot;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.batch.core.Job;
import org.springframework.expression.Expression;
import org.springframework.util.StringUtils;

import com.redis.lettucemod.search.Field;
import com.redis.lettucemod.search.IndexInfo;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.riot.core.RiotUtils;
import com.redis.riot.core.Step;
import com.redis.riot.faker.FakerItemReader;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "faker-import", description = "Import from Faker.")
public class FakerImport extends AbstractImport {

	@ArgGroup(exclusive = false)
	private FakerArgs fakerArgs = new FakerArgs();

	@ArgGroup(exclusive = false, heading = "Processor options%n")
	private ImportProcessorArgs processorArgs = new ImportProcessorArgs();

	public void copyTo(FakerImport target) {
		super.copyTo(target);
		target.fakerArgs = fakerArgs;
		target.processorArgs = processorArgs;
	}

	@Override
	protected Job job() {
		Step<Map<String, Object>, Map<String, Object>> step = new Step<>(reader(), mapWriter());
		step.processor(mapProcessor(processorArgs));
		step.maxItemCount(fakerArgs.getCount());
		step.taskName("Importing");
		return job(step);
	}

	private FakerItemReader reader() {
		FakerItemReader reader = new FakerItemReader();
		reader.setMaxItemCount(fakerArgs.getCount());
		reader.setLocale(fakerArgs.getLocale());
		reader.setFields(fields());
		return reader;
	}

	private Map<String, Expression> fields() {
		Map<String, Expression> allFields = new LinkedHashMap<>(fakerArgs.getFields());
		if (StringUtils.hasLength(fakerArgs.getSearchIndex())) {
			Map<String, Expression> searchFields = new LinkedHashMap<>();
			IndexInfo info = RedisModulesUtils.indexInfo(redisCommands.ftInfo(fakerArgs.getSearchIndex()));
			for (Field<String> field : info.getFields()) {
				searchFields.put(field.getName(), RiotUtils.parse(expression(field)));
			}
			allFields.putAll(searchFields);
		}
		return allFields;
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

	public FakerArgs getFakerArgs() {
		return fakerArgs;
	}

	public void setFakerArgs(FakerArgs args) {
		this.fakerArgs = args;
	}

}
