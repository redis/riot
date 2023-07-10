package com.redis.riot.cli;

import org.springframework.batch.core.Job;

import com.redis.riot.cli.common.AbstractStructImportCommand;
import com.redis.riot.cli.common.CommandContext;
import com.redis.riot.cli.common.GenerateOptions;
import com.redis.spring.batch.reader.GeneratorItemReader;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

@Command(name = "generate", description = "Generate data structures.")
public class Generate extends AbstractStructImportCommand {

	private static final String TASK_NAME = "Generating";

	@Mixin
	private GenerateOptions generateOptions = new GenerateOptions();

	public GenerateOptions getGenerateOptions() {
		return generateOptions;
	}

	public void setGenerateOptions(GenerateOptions options) {
		this.generateOptions = options;
	}

	@Override
	protected Job job(CommandContext context) {
		return job(step(reader(), writer(context)).task(TASK_NAME));
	}

	private GeneratorItemReader reader() {
		GeneratorItemReader reader = new GeneratorItemReader();
		reader.setMaxItemCount(generateOptions.getCount());
		reader.setExpiration(generateOptions.getExpiration());
		reader.setHashOptions(generateOptions.hashOptions());
		reader.setJsonOptions(generateOptions.jsonOptions());
		reader.setKeyRange(generateOptions.getKeyRange());
		reader.setKeyspace(generateOptions.getKeyspace());
		reader.setListOptions(generateOptions.listOptions());
		reader.setSetOptions(generateOptions.setOptions());
		reader.setStreamOptions(generateOptions.streamOptions());
		reader.setStringOptions(generateOptions.stringOptions());
		reader.setTimeSeriesOptions(generateOptions.timeSeriesOptions());
		reader.setTypes(generateOptions.getTypes());
		reader.setZsetOptions(generateOptions.zsetOptions());
		return reader;
	}

	@Override
	public String toString() {
		return "Generate [generateOptions=" + generateOptions + ", structOptions=" + structOptions + ", writerOptions="
				+ writerOptions + ", jobOptions=" + jobOptions + "]";
	}

}
