package com.redis.riot.cli;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;

import com.redis.riot.cli.common.AbstractCommand;
import com.redis.riot.cli.common.CommandContext;
import com.redis.riot.cli.common.ProgressMonitor;
import com.redis.riot.cli.common.RedisWriterOptions;
import com.redis.riot.cli.gen.DataStructureGeneratorOptions;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.reader.GeneratorItemReader;

import io.lettuce.core.codec.StringCodec;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

@Command(name = "generate", description = "Generate data structures")
public class Generate extends AbstractCommand {

	private static final Logger log = Logger.getLogger(Generate.class.getName());

	@Mixin
	private DataStructureGeneratorOptions options = new DataStructureGeneratorOptions();

	@ArgGroup(exclusive = false, heading = "Writer options%n")
	private RedisWriterOptions writerOptions = new RedisWriterOptions();

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
	protected Job job(CommandContext context) {
		RedisItemWriter<String, String, DataStructure<String>> writer = context.dataStructureWriter(StringCodec.UTF8)
				.options(writerOptions.writerOptions()).dataStructureOptions(writerOptions.dataStructureOptions())
				.build();
		log.log(Level.FINE, "Creating random data structure reader with {0}", options);
		GeneratorItemReader reader = new GeneratorItemReader(options.generatorOptions());
		options.configure(reader);
		SimpleStepBuilder<DataStructure<String>, DataStructure<String>> step = step(context, reader, null, writer);
		ProgressMonitor monitor = progressMonitor().initialMax(options.getCount()).task("Generating").build();
		return context.getJobRunner().job(commandName()).start(step(step, monitor).build()).build();
	}

}
