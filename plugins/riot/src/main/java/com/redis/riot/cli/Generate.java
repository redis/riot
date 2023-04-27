package com.redis.riot.cli;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;

import com.redis.riot.cli.common.AbstractTransferCommand;
import com.redis.riot.cli.common.JobCommandContext;
import com.redis.riot.cli.common.ProgressMonitor;
import com.redis.riot.cli.common.RedisOptions;
import com.redis.riot.cli.common.RedisWriterOptions;
import com.redis.riot.cli.gen.DataStructureGeneratorOptions;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.reader.GeneratorItemReader;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.ParentCommand;

@Command(name = "generate", description = "Generate data structures")
public class Generate extends AbstractTransferCommand {

	private static final Logger log = Logger.getLogger(Generate.class.getName());

	private static final String COMMAND_NAME = "generate";

	@ParentCommand
	private Riot parent;

	@Mixin
	private DataStructureGeneratorOptions options = new DataStructureGeneratorOptions();

	@ArgGroup(exclusive = false, heading = "Writer options%n")
	private RedisWriterOptions writerOptions = new RedisWriterOptions();

	@Override
	protected RedisOptions getRedisOptions() {
		return parent.getRedisOptions();
	}

	public DataStructureGeneratorOptions getOptions() {
		return options;
	}

	public void setOptions(DataStructureGeneratorOptions options) {
		this.options = options;
	}

	public RedisWriterOptions getWriterOptions() {
		return writerOptions;
	}

	public void setWriterOptions(RedisWriterOptions writerOptions) {
		this.writerOptions = writerOptions;
	}

	@Override
	protected Job job(JobCommandContext context) {
		RedisItemWriter<String, String, DataStructure<String>> writer = context.writer()
				.options(writerOptions.writerOptions()).dataStructure();
		log.log(Level.FINE, "Creating random data structure reader with {0}", options);
		GeneratorItemReader reader = new GeneratorItemReader(options.generatorOptions());
		options.configure(reader);
		SimpleStepBuilder<DataStructure<String>, DataStructure<String>> step = step(context, COMMAND_NAME, reader, null,
				writer);
		ProgressMonitor monitor = options.configure(progressMonitor()).task("Generating").build();
		return context.job(COMMAND_NAME).start(step(step, monitor).build()).build();

	}

}
