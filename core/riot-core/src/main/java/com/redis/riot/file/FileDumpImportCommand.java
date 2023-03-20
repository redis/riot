package com.redis.riot.file;

import java.util.Iterator;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.core.io.Resource;

import com.redis.riot.AbstractTransferCommand;
import com.redis.riot.JobCommandContext;
import com.redis.riot.ProgressMonitor;
import com.redis.riot.RedisWriterOptions;
import com.redis.riot.file.resource.XmlItemReader;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.common.DataStructure;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

@Command(name = "file-dump-import", description = "Import Redis data files into Redis")
public class FileDumpImportCommand extends AbstractTransferCommand {

	private static final String COMMAND_NAME = "file-dump-import";
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
	protected Job job(JobCommandContext context) {
		Iterator<TaskletStep> stepIterator = options.getResources().map(r -> step(context, r)).iterator();
		SimpleJobBuilder job = context.job(COMMAND_NAME).start(stepIterator.next());
		while (stepIterator.hasNext()) {
			job.next(stepIterator.next());
		}
		return job.build();
	}

	public TaskletStep step(JobCommandContext context, Resource resource) {
		String name = String.join("-", COMMAND_NAME, resource.getDescription());
		ItemReader<DataStructure<String>> reader = reader(resource);
		if (reader instanceof ItemStreamSupport) {
			((ItemStreamSupport) reader).setName(name);
		}
		DataStructureProcessor processor = new DataStructureProcessor();
		ProgressMonitor monitor = progressMonitor().task("Importing " + resource).build();
		RedisItemWriter<String, String, DataStructure<String>> writer = context.writer()
				.options(writerOptions.writerOptions()).dataStructure();
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
