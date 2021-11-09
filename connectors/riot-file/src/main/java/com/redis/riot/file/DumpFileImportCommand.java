package com.redis.riot.file;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.batch.item.xml.XmlItemReader;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import com.redis.riot.AbstractTransferCommand;
import com.redis.riot.RedisOptions;
import com.redis.riot.RedisWriterOptions;
import com.redis.riot.RiotStepBuilder;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.RedisItemWriter.DataStructureItemWriterBuilder;
import com.redis.spring.batch.support.DataStructure;

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
		DataStructureItemProcessor processor = new DataStructureItemProcessor();
		for (String file : expandedFiles) {
			DumpFileType fileType = fileType(file);
			Resource resource = options.inputResource(file);
			AbstractItemStreamItemReader<DataStructure<String>> reader = reader(fileType, resource);
			reader.setName(file + "-" + NAME + "-reader");
			RiotStepBuilder<DataStructure<String>, DataStructure<String>> step = riotStep(file + "-" + NAME,
					"Importing " + file);
			steps.add(step.reader(reader).processor(processor).writer(writer()).build().build());
		}
		return flow(NAME, steps.toArray(new Step[0]));
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
