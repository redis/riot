package com.redis.riot.cli;

import java.util.Iterator;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.core.io.Resource;

import com.redis.riot.cli.common.AbstractCommand;
import com.redis.riot.cli.common.CommandContext;
import com.redis.riot.cli.common.ProgressMonitor;
import com.redis.riot.cli.common.RedisWriterOptions;
import com.redis.riot.cli.file.FileDumpOptions;
import com.redis.riot.cli.file.FileImportOptions;
import com.redis.riot.core.FileDumpType;
import com.redis.riot.core.FileUtils;
import com.redis.riot.core.processor.DataStructureProcessor;
import com.redis.riot.core.resource.XmlItemReader;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.common.DataStructure;

import io.lettuce.core.codec.StringCodec;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

@Command(name = "dump-import", description = "Import Redis data files into Redis.")
public class DumpImport extends AbstractCommand {

	@Mixin
	private FileImportOptions options = new FileImportOptions();
	@Mixin
	private FileDumpOptions dumpFileOptions = new FileDumpOptions();
	@ArgGroup(exclusive = false, heading = "Writer options%n")
	private RedisWriterOptions writerOptions = new RedisWriterOptions();

	public FileDumpOptions getDumpFileOptions() {
		return dumpFileOptions;
	}

	public void setDumpFileOptions(FileDumpOptions dumpFileOptions) {
		this.dumpFileOptions = dumpFileOptions;
	}

	public FileImportOptions getOptions() {
		return options;
	}

	public void setOptions(FileImportOptions options) {
		this.options = options;
	}

	@Override
	protected Job job(CommandContext context) {
		Iterator<TaskletStep> stepIterator = options.getResources().map(r -> step(context, r)).iterator();
		SimpleJobBuilder job = context.getJobRunner().job(commandName()).start(stepIterator.next());
		while (stepIterator.hasNext()) {
			job.next(stepIterator.next());
		}
		return job.build();
	}

	public TaskletStep step(CommandContext context, Resource resource) {
		String name = String.join("-", commandName(), resource.getDescription());
		ItemReader<DataStructure<String>> reader = reader(resource);
		DataStructureProcessor processor = new DataStructureProcessor();
		ProgressMonitor monitor = progressMonitor().task("Importing " + resource).build();
		RedisItemWriter<String, String, DataStructure<String>> writer = context.dataStructureWriter(StringCodec.UTF8)
				.options(writerOptions.writerOptions()).dataStructureOptions(writerOptions.dataStructureOptions())
				.build();
		return step(step(context, name, reader, processor, writer), monitor).build();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected ItemReader<DataStructure<String>> reader(Resource resource) {
		FileDumpType fileType = dumpFileOptions.type(resource);
		if (fileType == FileDumpType.XML) {
			return (XmlItemReader) FileUtils.xmlReader(resource, DataStructure.class);
		}
		return (JsonItemReader) FileUtils.jsonReader(resource, DataStructure.class);
	}

}
