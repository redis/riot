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
import com.redis.spring.batch.writer.WriterBuilder;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

@Command(name = "file-dump-import", description = "Import Redis data files into Redis")
public class FileDumpImportCommand extends AbstractTransferCommand {

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
	protected Job job(JobCommandContext context) throws Exception {
		Iterator<TaskletStep> stepIterator = options.getResources().map(r -> step(context, r)).iterator();
		SimpleJobBuilder job = job(context, commandSpec.name(), stepIterator.next());
		while (stepIterator.hasNext()) {
			job.next(stepIterator.next());
		}
		return job.build();
	}

	public TaskletStep step(JobCommandContext context, Resource resource) {
		String name = resource.getDescription() + "-" + commandSpec.name();
		ItemReader<DataStructure<String>> reader = reader(resource);
		if (reader instanceof ItemStreamSupport) {
			((ItemStreamSupport) reader).setName(name);
		}
		DataStructureProcessor processor = new DataStructureProcessor();
		ProgressMonitor monitor = progressMonitor().task("Importing " + resource).build();
		RedisItemWriter<String, String, DataStructure<String>> writer = writer(context)
				.options(writerOptions.writerOptions()).build();
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

	protected WriterBuilder<String, String, DataStructure<String>> writer(JobCommandContext context) {
		return RedisItemWriter.dataStructure(context.pool());
	}

}
