package com.redis.riot.file;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import com.redis.riot.AbstractTransferCommand;
import com.redis.riot.JobCommandContext;
import com.redis.riot.ProgressMonitor;
import com.redis.riot.RedisWriterOptions;
import com.redis.riot.file.resource.XmlItemReader;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.DataStructure.Type;

import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;

@Command(name = "import-dump", description = "Import Redis data files into Redis")
public class DumpFileImportCommand extends AbstractTransferCommand {

	private static final String NAME = "dump-file-import";

	@Parameters(arity = "0..*", description = "One ore more files or URLs", paramLabel = "FILE")
	private List<String> files;

	@Mixin
	private DumpFileOptions options = new DumpFileOptions();

	@ArgGroup(exclusive = false, heading = "Writer options%n")
	private RedisWriterOptions writerOptions = new RedisWriterOptions();

	public List<String> getFiles() {
		return files;
	}

	public void setFiles(List<String> files) {
		this.files = files;
	}

	public DumpFileOptions getOptions() {
		return options;
	}

	public void setOptions(DumpFileOptions options) {
		this.options = options;
	}

	public RedisWriterOptions getWriterOptions() {
		return writerOptions;
	}

	public void setWriterOptions(RedisWriterOptions writerOptions) {
		this.writerOptions = writerOptions;
	}

	@Override
	protected Job job(JobCommandContext context) throws Exception {
		Assert.isTrue(!ObjectUtils.isEmpty(files), "No file specified");
		List<TaskletStep> steps = new ArrayList<>();
		for (String file : files) {
			for (String expandedFile : FileUtils.expand(file)) {
				String name = expandedFile + "-" + NAME;
				Resource resource = options.inputResource(expandedFile);
				AbstractItemStreamItemReader<DataStructure<String>> reader = reader(options.type(resource), resource);
				reader.setName(name);
				ProgressMonitor monitor = progressMonitor().task("Importing " + expandedFile).build();
				DataStructureProcessor processor = new DataStructureProcessor();
				RedisItemWriter<String, String, DataStructure<String>> writer = RedisItemWriter
						.dataStructure(context.pool()).options(writerOptions.writerOptions()).build();
				steps.add(step(step(context, name, reader, processor, writer), monitor).build());
			}
		}
		Iterator<TaskletStep> stepIterator = steps.iterator();
		SimpleJobBuilder job = job(context, NAME, stepIterator.next());
		while (stepIterator.hasNext()) {
			job.next(stepIterator.next());
		}
		return job.build();
	}

	private static class DataStructureProcessor implements ItemProcessor<DataStructure<String>, DataStructure<String>> {

		@SuppressWarnings("unchecked")
		@Override
		public DataStructure<String> process(DataStructure<String> item) throws Exception {
			if (item.getType() == null) {
				return item;
			}
			Type type = item.getType();
			if (type == Type.ZSET) {
				Collection<Map<String, Object>> zset = (Collection<Map<String, Object>>) item.getValue();
				Collection<ScoredValue<String>> values = new ArrayList<>(zset.size());
				for (Map<String, Object> map : zset) {
					double score = ((Number) map.get("score")).doubleValue();
					String value = (String) map.get("value");
					values.add((ScoredValue<String>) ScoredValue.fromNullable(score, value));
				}
				item.setValue(values);
				return item;
			}
			if (type == Type.STREAM) {
				Collection<Map<String, Object>> stream = (Collection<Map<String, Object>>) item.getValue();
				Collection<StreamMessage<String, String>> messages = new ArrayList<>(stream.size());
				for (Map<String, Object> message : stream) {
					messages.add(new StreamMessage<>((String) message.get("stream"), (String) message.get("id"),
							(Map<String, String>) message.get("body")));
				}
				item.setValue(messages);
				return item;
			}
			return item;
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected AbstractItemStreamItemReader<DataStructure<String>> reader(DumpFileType fileType, Resource resource) {
		if (fileType == DumpFileType.XML) {
			return (XmlItemReader) FileUtils.xmlReader(resource, DataStructure.class);
		}
		return (JsonItemReader) FileUtils.jsonReader(resource, DataStructure.class);
	}

}
