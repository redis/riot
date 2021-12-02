package com.redis.riot.file;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import com.redis.riot.AbstractTransferCommand;
import com.redis.riot.RedisOptions;
import com.redis.riot.RedisWriterOptions;
import com.redis.riot.RiotStepBuilder;
import com.redis.riot.file.resource.XmlItemReader;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.RedisItemWriter.DataStructureItemWriterBuilder;
import com.redis.spring.batch.support.DataStructure;

import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "import-dump", description = "Import Redis data files into Redis")
public class DumpFileImportCommand extends AbstractTransferCommand {

	private static final Logger log = LoggerFactory.getLogger(DumpFileImportCommand.class);

	private static final String NAME = "dump-file-import";

	@CommandLine.Parameters(arity = "0..*", description = "One ore more files or URLs", paramLabel = "FILE")
	private List<String> files;
	@CommandLine.Mixin
	private DumpFileImportOptions options = new DumpFileImportOptions();
	@ArgGroup(exclusive = false, heading = "Writer options%n")
	private RedisWriterOptions writerOptions = new RedisWriterOptions();

	public List<String> getFiles() {
		return files;
	}

	public void setFiles(List<String> files) {
		this.files = files;
	}

	public DumpFileImportOptions getOptions() {
		return options;
	}

	public RedisWriterOptions getWriterOptions() {
		return writerOptions;
	}

	@Override
	protected Flow flow() throws Exception {
		Assert.isTrue(!ObjectUtils.isEmpty(files), "No file specified");
		List<String> expandedFiles = FileUtils.expand(files);
		if (ObjectUtils.isEmpty(expandedFiles)) {
			throw new FileNotFoundException("File not found: " + String.join(", ", files));
		}
		List<Step> steps = new ArrayList<>();
		for (String file : expandedFiles) {
			DumpFileType fileType = fileType(file);
			Resource resource = options.inputResource(file);
			AbstractItemStreamItemReader<DataStructure<String>> reader = reader(fileType, resource);
			reader.setName(file + "-" + NAME + "-reader");
			RiotStepBuilder<DataStructure<String>, DataStructure<String>> step = riotStep(file + "-" + NAME,
					"Importing " + file);
			steps.add(step.reader(reader).processor(this::processDataStructure).writer(writer()).build().build());
		}
		return flow(NAME, steps.toArray(new Step[0]));
	}

	@SuppressWarnings("unchecked")
	private DataStructure<String> processDataStructure(DataStructure<String> item) {
		String type = item.getType().toLowerCase();
		if (DataStructure.ZSET.equals(type)) {
			Collection<Map<String, Object>> zset = (Collection<Map<String, Object>>) item.getValue();
			Collection<ScoredValue<String>> values = new ArrayList<>(zset.size());
			for (Map<String, Object> map : zset) {
				double score = ((Number) map.get("score")).doubleValue();
				String value = (String) map.get("value");
				values.add((ScoredValue<String>) ScoredValue.fromNullable(score, value));
			}
			item.setValue(values);
		} else if (DataStructure.STREAM.equals(type)) {
			Collection<Map<String, Object>> stream = (Collection<Map<String, Object>>) item.getValue();
			Collection<StreamMessage<String, String>> messages = new ArrayList<>(stream.size());
			for (Map<String, Object> message : stream) {
				messages.add(new StreamMessage<>((String) message.get("stream"), (String) message.get("id"),
						(Map<String, String>) message.get("body")));
			}
			item.setValue(messages);
		}
		return item;
	}

	private DumpFileType fileType(String file) {
		if (options.getType() == null) {
			return DumpFileType.of(file);
		}
		return options.getType();
	}

	private ItemWriter<DataStructure<String>> writer() {
		return writerOptions.configureWriter(dataStructureWriter(getRedisOptions())).build();
	}

	private DataStructureItemWriterBuilder<String, String> dataStructureWriter(RedisOptions options) {
		if (options.isCluster()) {
			return RedisItemWriter.client(options.clusterClient()).dataStructure();
		}
		return RedisItemWriter.client(options.client()).dataStructure();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected AbstractItemStreamItemReader<DataStructure<String>> reader(DumpFileType fileType, Resource resource) {
		if (fileType == DumpFileType.XML) {
			log.debug("Creating XML data structure reader for file {}", resource);
			return (XmlItemReader) FileUtils.xmlReader(resource, DataStructure.class);
		}
		log.debug("Creating JSON data structure reader for file {}", resource);
		return (JsonItemReader) FileUtils.jsonReader(resource, DataStructure.class);
	}

}
