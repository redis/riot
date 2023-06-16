package com.redis.riot.cli;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;

import com.redis.riot.cli.common.AbstractCommand;
import com.redis.riot.cli.common.CommandContext;
import com.redis.riot.cli.common.GenerateOptions;
import com.redis.riot.cli.common.RedisWriterOptions;
import com.redis.riot.cli.common.StepProgressMonitor;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.reader.GeneratorItemReader;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

@Command(name = "generate", description = "Generate data structures.")
public class Generate extends AbstractCommand {

	private static final Logger log = Logger.getLogger(Generate.class.getName());

	private static final String TASK_NAME = "Generating";

	@Mixin
	private GenerateOptions options = new GenerateOptions();

	@ArgGroup(exclusive = false, heading = "Writer options%n")
	private RedisWriterOptions writerOptions = new RedisWriterOptions();

	public GenerateOptions getOptions() {
		return options;
	}

	public void setOptions(GenerateOptions options) {
		this.options = options;
	}

	public RedisWriterOptions getWriterOptions() {
		return writerOptions;
	}

	public void setWriterOptions(RedisWriterOptions writerOptions) {
		this.writerOptions = writerOptions;
	}

	@Override
	protected Job job(CommandContext context) {
		log.log(Level.FINE, "Creating random data structure reader with {0}", options);
		SimpleStepBuilder<DataStructure<String>, DataStructure<String>> step = step();
		GeneratorItemReader reader = new GeneratorItemReader();
		reader.setMaxItemCount(options.getCount());
		reader.withExpiration(options.getExpiration());
		reader.withHashOptions(options.hashOptions());
		reader.withJsonOptions(options.jsonOptions());
		reader.withKeyRange(options.getKeyRange());
		reader.withKeyspace(options.getKeyspace());
		reader.withListOptions(options.listOptions());
		reader.withSetOptions(options.setOptions());
		reader.withStreamOptions(options.streamOptions());
		reader.withStringOptions(options.stringOptions());
		reader.withTimeSeriesOptions(options.timeSeriesOptions());
		reader.withTypes(options.getTypes());
		reader.withZsetOptions(options.zsetOptions());
		step.reader(reader);
		step.writer(writer(context.getRedisClient(), writerOptions).dataStructure());
		StepProgressMonitor monitor = progressMonitor(TASK_NAME);
		monitor.withInitialMax(options.getCount());
		monitor.register(step);
		return job(commandName()).start(step.build()).build();
	}

}
