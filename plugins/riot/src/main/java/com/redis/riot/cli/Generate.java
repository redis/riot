package com.redis.riot.cli;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemWriter;

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
		TaskletStep step = step(commandName(), reader(), writer(context)).build();
		StepProgressMonitor monitor = monitor(TASK_NAME);
		monitor.withInitialMax(options.getCount());
		monitor.register(step);
		return job(step);
	}

	private ItemWriter<DataStructure<String>> writer(CommandContext context) {
		return writer(context.getRedisClient(), writerOptions).dataStructure();
	}

	private GeneratorItemReader reader() {
		GeneratorItemReader reader = new GeneratorItemReader();
		reader.setMaxItemCount(options.getCount());
		reader.setExpiration(options.getExpiration());
		reader.setHashOptions(options.hashOptions());
		reader.setJsonOptions(options.jsonOptions());
		reader.setKeyRange(options.getKeyRange());
		reader.setKeyspace(options.getKeyspace());
		reader.setListOptions(options.listOptions());
		reader.setSetOptions(options.setOptions());
		reader.setStreamOptions(options.streamOptions());
		reader.setStringOptions(options.stringOptions());
		reader.setTimeSeriesOptions(options.timeSeriesOptions());
		reader.setTypes(options.getTypes());
		reader.setZsetOptions(options.zsetOptions());
		return reader;
	}

}
