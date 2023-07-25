package com.redis.riot.cli;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.core.io.Resource;

import com.redis.riot.cli.common.AbstractDataStructureImportCommand;
import com.redis.riot.cli.common.CommandContext;
import com.redis.riot.cli.file.DumpOptions;
import com.redis.riot.cli.file.FileOptions;
import com.redis.riot.core.FileDumpType;
import com.redis.riot.core.FileUtils;
import com.redis.riot.core.resource.XmlItemReader;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.common.KeyValue;

import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;

@Command(name = "dump-import", description = "Import Redis data files into Redis.")
public class DumpImport extends AbstractDataStructureImportCommand {

	@Parameters(arity = "0..*", description = "One ore more files or URLs", paramLabel = "FILE")
	protected List<String> files = new ArrayList<>();

	@Mixin
	private FileOptions fileOptions = new FileOptions();

	@Mixin
	private DumpOptions dumpOptions = new DumpOptions();

	public List<String> getFiles() {
		return files;
	}

	public void setFiles(List<String> files) {
		this.files = files;
	}

	public DumpOptions getDumpOptions() {
		return dumpOptions;
	}

	public void setDumpOptions(DumpOptions dumpFileOptions) {
		this.dumpOptions = dumpFileOptions;
	}

	public FileOptions getFileOptions() {
		return fileOptions;
	}

	public void setFileOptions(FileOptions options) {
		this.fileOptions = options;
	}

	@Override
	protected Job job(CommandContext context) {
		Iterator<TaskletStep> steps = FileUtils.expandAll(files).map(fileOptions::inputResource)
				.map(r -> step(context, r)).map(SimpleStepBuilder::build).iterator();
		if (!steps.hasNext()) {
			throw new IllegalArgumentException("No files found");
		}
		SimpleJobBuilder job = job(commandName()).start(steps.next());
		while (steps.hasNext()) {
			job.next(steps.next());
		}
		return job.build();
	}

	public SimpleStepBuilder<KeyValue<String>, KeyValue<String>> step(CommandContext context, Resource resource) {
		String name = commandName() + "-" + resource.getDescription();
		RedisItemWriter<String, String> writer = writer(context);
		String task = "Importing " + resource;
		return step(reader(resource), writer).name(name).processor(this::process).task(task).build();
	}

	private KeyValue<String> process(KeyValue<String> item) {
		if (item.getType() == null) {
			return null;
		}
		item.setValue(value(item));
		return item;
	}

	@SuppressWarnings("unchecked")
	private Object value(KeyValue<String> item) {
		switch (item.getType()) {
		case KeyValue.ZSET:
			Collection<Map<String, Object>> zset = (Collection<Map<String, Object>>) item.getValue();
			Collection<ScoredValue<String>> values = new ArrayList<>(zset.size());
			for (Map<String, Object> map : zset) {
				double score = ((Number) map.get("score")).doubleValue();
				String value = (String) map.get("value");
				values.add((ScoredValue<String>) ScoredValue.fromNullable(score, value));
			}
			return values;
		case KeyValue.STREAM:
			Collection<Map<String, Object>> stream = (Collection<Map<String, Object>>) item.getValue();
			Collection<StreamMessage<String, String>> messages = new ArrayList<>(stream.size());
			for (Map<String, Object> message : stream) {
				messages.add(new StreamMessage<>((String) message.get("stream"), (String) message.get("id"),
						(Map<String, String>) message.get("body")));
			}
			return messages;
		default:
			return item.getValue();
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected ItemReader<KeyValue<String>> reader(Resource resource) {
		FileDumpType fileType = dumpOptions.type(resource);
		if (fileType == FileDumpType.XML) {
			return (XmlItemReader) FileUtils.xmlReader(resource, KeyValue.class);
		}
		return (JsonItemReader) FileUtils.jsonReader(resource, KeyValue.class);
	}

	@Override
	public String toString() {
		return "DumpImport [files=" + files + ", fileOptions=" + fileOptions + ", dumpOptions=" + dumpOptions
				+ ", operationOptions=" + writerOptions + ", operationOptions=" + writerOptions + ", jobOptions=" + jobOptions
				+ "]";
	}

}
