package com.redislabs.recharge;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder.DelimitedBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder.FixedLengthBuilder;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.util.ResourceUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redislabs.recharge.RechargeConfiguration.DelimitedConfiguration;
import com.redislabs.recharge.RechargeConfiguration.EntityConfiguration;
import com.redislabs.recharge.RechargeConfiguration.FileEntityConfiguration;
import com.redislabs.recharge.RechargeConfiguration.FileType;
import com.redislabs.recharge.RechargeConfiguration.FixedLengthConfiguration;
import com.redislabs.recharge.RechargeConfiguration.IndexConfiguration;
import com.redislabs.recharge.file.MapFieldSetMapper;
import com.redislabs.recharge.file.json.JsonEntityReader;
import com.redislabs.recharge.generator.GeneratorEntityItemReader;
import com.redislabs.recharge.redis.HashWriter;
import com.redislabs.recharge.redis.NilWriter;
import com.redislabs.recharge.redis.StringWriter;
import com.redislabs.recharge.redis.index.AbstractIndexWriter;
import com.redislabs.recharge.redis.index.GeoIndexWriter;
import com.redislabs.recharge.redis.index.ListIndexWriter;
import com.redislabs.recharge.redis.index.RediSearchIndexWriter;
import com.redislabs.recharge.redis.index.SetIndexWriter;
import com.redislabs.recharge.redis.index.ZSetIndexWriter;

import lombok.extern.slf4j.Slf4j;

@SpringBootApplication
@EnableBatchProcessing
@Slf4j
public class RechargeApplication implements ApplicationRunner {

	private static final String FILE_BASENAME = "basename";
	private static final String FILE_EXTENSION = "extension";
	private static final String FILE_GZ = "gz";

	public static void main(String[] args) {
		ConfigurableApplicationContext context = SpringApplication.run(RechargeApplication.class, args);
		SpringApplication.exit(context);
	}

	@Autowired
	private JobLauncher jobLauncher;
	@Autowired
	private JobBuilderFactory jobBuilderFactory;
	@Autowired
	private StepBuilderFactory stepFactory;
	@Autowired
	private RechargeConfiguration config;
	@Autowired
	private RedisProperties redisConfig;
	@Autowired
	private StringRedisTemplate redisTemplate;

	private List<Flow> getLoadFlows() throws Exception {
		List<Flow> flows = new ArrayList<>();
		Map<String, AbstractItemCountingItemStreamItemReader<Entity>> readers = getLoadReaders();
		for (String entity : readers.keySet()) {
			AbstractItemCountingItemStreamItemReader<Entity> reader = readers.get(entity);
			SimpleStepBuilder<Entity, Entity> step = stepFactory.get(entity + "-load-step")
					.<Entity, Entity>chunk(config.getChunkSize());
			if (config.getMaxItemCount() != -1) {
				reader.setMaxItemCount(config.getMaxItemCount());
			}
			step.reader(reader);
			EntityConfiguration entityConfig = config.getEntities().getOrDefault(entity, new EntityConfiguration());
			step.writer(getWriter(entityConfig));
			step.throttleLimit(config.getMaxThreads());
			flows.add(new FlowBuilder<Flow>(entity + "-load-flow").from(step.build()).end());
		}
		return flows;
	}

	private ItemWriter<Entity> getWriter(EntityConfiguration entityConfig) {
		ItemWriter<Entity> writer = getEntityWriter(entityConfig);
		if (entityConfig.getIndexes().size() > 0) {
			List<ItemWriter<? super Entity>> writers = new ArrayList<>();
			writers.add(writer);
			for (IndexConfiguration indexConfig : entityConfig.getIndexes()) {
				writers.add(getIndexWriter(entityConfig, indexConfig));
			}
			CompositeItemWriter<Entity> composite = new CompositeItemWriter<>();
			composite.setDelegates(writers);
			return composite;
		}
		return writer;
	}

	private AbstractIndexWriter getIndexWriter(EntityConfiguration entityConfig, IndexConfiguration indexConfig) {
		switch (indexConfig.getType()) {
		case Geo:
			return new GeoIndexWriter(entityConfig, redisTemplate, indexConfig);
		case List:
			return new ListIndexWriter(entityConfig, redisTemplate, indexConfig);
		case Search:
			return new RediSearchIndexWriter(entityConfig, redisTemplate, indexConfig, redisConfig);
		case Zset:
			return new ZSetIndexWriter(entityConfig, redisTemplate, indexConfig);
		default:
			return new SetIndexWriter(entityConfig, redisTemplate, indexConfig);
		}
	}

	private Map<String, AbstractItemCountingItemStreamItemReader<Entity>> getLoadReaders() throws Exception {
		Map<String, AbstractItemCountingItemStreamItemReader<Entity>> readers = new LinkedHashMap<>();
		for (Entry<String, EntityConfiguration> entity : config.getEntities().entrySet()) {
			readers.put(entity.getKey(), getReader(entity));
			EntityConfiguration entityConfig = entity.getValue();
			if (entityConfig.getKeys().isEmpty()) {
				entity.getValue().setKeys(entityConfig.getFields());
			}
			for (IndexConfiguration indexConfig : entityConfig.getIndexes()) {
				if (indexConfig.getField() == null) {
					if (entityConfig.getFields().size() > 1) {
						indexConfig.setField(entityConfig.getFields().get(1));
					} else {
						log.error("Could not find a field to index");
					}
				}
				if (indexConfig.getScore() == null) {
					indexConfig.setScore(entityConfig.getKeys().get(0));
				}
			}
		}
		return readers;
	}

	private AbstractItemCountingItemStreamItemReader<Entity> getReader(Entry<String, EntityConfiguration> entity)
			throws Exception {
		if (entity.getValue().getGenerator() != null) {
			return new GeneratorEntityItemReader(entity);
		}
		if (entity.getValue().getFile() != null) {
			FileEntityConfiguration fileConfig = entity.getValue().getFile();
			if (fileConfig.getType() == null) {
				fileConfig.setType(guessFileType(fileConfig.getPath()));
			}
			switch (fileConfig.getType()) {
			case Delimited:
				return getDelimitedReader(entity);
			case FixedLength:
				return getFixedLengthReader(entity);
			case Json:
				return new JsonEntityReader(entity);
			default:
				throw new RechargeException("No reader found for file " + fileConfig.getPath());
			}
		}
		throw new RechargeException("No entity type configured");
	}

	private FlatFileItemReaderBuilder<Entity> getFlatFileReaderBuilder(String entityName,
			FileEntityConfiguration fileConfig) throws IOException {
		FlatFileItemReaderBuilder<Entity> builder = new FlatFileItemReaderBuilder<>();
		builder.name(entityName + "-file-reader");
		Resource resource = getResource(fileConfig);
		builder.resource(resource);
		if (fileConfig.getEncoding() != null) {
			builder.encoding(fileConfig.getEncoding());
		}
		builder.strict(true);
		builder.saveState(false);
		builder.fieldSetMapper(new MapFieldSetMapper(entityName));
		if (fileConfig.getLinesToSkip() != null) {
			builder.linesToSkip(fileConfig.getLinesToSkip());
		}
		return builder;
	}

	private FlatFileItemReader<Entity> getDelimitedReader(Entry<String, EntityConfiguration> entity)
			throws IOException {
		FileEntityConfiguration fileConfig = entity.getValue().getFile();
		FlatFileItemReaderBuilder<Entity> builder = getFlatFileReaderBuilder(entity.getKey(), fileConfig);
		DelimitedConfiguration config = fileConfig.getDelimited();
		DelimitedBuilder<Entity> delimitedBuilder = builder.delimited();
		if (config.getDelimiter() != null) {
			delimitedBuilder.delimiter(config.getDelimiter());
		}
		if (config.getIncludedFields() != null) {
			delimitedBuilder.includedFields(config.getIncludedFields());
		}
		if (config.getQuoteCharacter() != null) {
			delimitedBuilder.quoteCharacter(config.getQuoteCharacter());
		}
		delimitedBuilder.names(entity.getValue().getFields().toArray(new String[0]));
		return builder.build();
	}

	protected FlatFileItemReader<Entity> getFixedLengthReader(Entry<String, EntityConfiguration> entity)
			throws IOException {
		FileEntityConfiguration fileConfig = entity.getValue().getFile();
		FlatFileItemReaderBuilder<Entity> builder = getFlatFileReaderBuilder(entity.getKey(), fileConfig);
		FixedLengthConfiguration config = fileConfig.getFixedLength();
		FixedLengthBuilder<Entity> fixedLengthBuilder = builder.fixedLength();
		if (config.getRanges() != null) {
			fixedLengthBuilder.columns(getRanges(config.getRanges()));
		}
		if (config.getStrict() != null) {
			fixedLengthBuilder.strict(config.getStrict());
		}
		fixedLengthBuilder.names(entity.getValue().getFields().toArray(new String[0]));
		return builder.build();
	}

	private Range[] getRanges(String[] strings) {
		Range[] ranges = new Range[strings.length];
		for (int index = 0; index < strings.length; index++) {
			ranges[index] = getRange(strings[index]);
		}
		return ranges;
	}

	private Range getRange(String string) {
		String[] split = string.split("-");
		return new Range(Integer.parseInt(split[0]), Integer.parseInt(split[1]));
	}

//	private String getBaseFilename(Resource resource) {
//		String filename = resource.getFilename();
//		int pos = filename.indexOf(".");
//		if (pos == -1) {
//			return filename;
//		}
//		return filename.substring(0, pos);
//	}

	private FileType guessFileType(String path) {
		String extension = getFilenameGroup(path, FILE_EXTENSION);
		if (extension == null) {
			log.warn("Could not determine file type from path {}", path);
		}
		log.debug("Found file extension '{}' for path {}", extension, path);
		return config.getFileTypes().getOrDefault(extension, FileType.Delimited);
	}

	private Pattern filePathPattern = Pattern
			.compile("(?<" + FILE_BASENAME + ">.+)\\.(?<" + FILE_EXTENSION + ">\\w+)(?<" + FILE_GZ + ">\\.gz)?");

	private boolean isGzip(FileEntityConfiguration config) {
		if (config.getGzip() == null) {
			String gz = getFilenameGroup(config.getPath(), FILE_GZ);
			return gz != null && gz.length() > 0;
		}
		return config.getGzip();
	}

	private String getFilenameGroup(String path, String groupName) {
		Matcher matcher = filePathPattern.matcher(getFilename(path));
		if (matcher.find()) {
			return matcher.group(groupName);
		}
		return null;
	}

	private String getFilename(String path) {
		return new File(path).getName();
	}

	private Resource getResource(FileEntityConfiguration config) throws IOException {
		Resource resource = getResource(config.getPath());
		if (isGzip(config)) {
			return getGZipResource(resource);
		}
		return resource;
	}

	private Resource getGZipResource(Resource resource) throws IOException {
		return new InputStreamResource(new GZIPInputStream(resource.getInputStream()));
	}

	private Resource getResource(String path) throws MalformedURLException {
		if (ResourceUtils.isUrl(path)) {
			return new UrlResource(path);
		}
		return new FileSystemResource(path);
	}

	private AbstractItemStreamItemWriter<Entity> getEntityWriter(EntityConfiguration config) {
		switch (config.getType()) {
		case Nil:
			return new NilWriter();
		case String:
			return new StringWriter(config, redisTemplate, getObjectWriter(config));
		default:
			return new HashWriter(config, redisTemplate);
		}
	}

	private ObjectWriter getObjectWriter(EntityConfiguration config) {
		switch (config.getFormat()) {
		case Xml:
			return new XmlMapper().writer().withRootName(config.getXml().getRootName());
		default:
			break;
		}
		return new ObjectMapper().writer();
	}

	@Override
	public void run(ApplicationArguments args) throws Exception {
		List<String> nonOptionArgs = args.getNonOptionArgs();
		if (nonOptionArgs.size() > 0) {
			String command = nonOptionArgs.get(0);
			List<Flow> flows = getFlows(command);
			SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor();
			executor.setConcurrencyLimit(config.getMaxThreads());
			Flow flow = new FlowBuilder<Flow>("split-flow").split(executor).add(flows.toArray(new Flow[flows.size()]))
					.build();
			Job job = jobBuilderFactory.get(command + "-job").start(flow).end().build();
			jobLauncher.run(job, new JobParameters());
		} else {
			log.error("No command given. Run 'recharge help' for usage");
		}
	}

	private List<Flow> getFlows(String command) throws Exception {
		switch (command) {
		case "load":
			return getLoadFlows();
		case "unload":
			return getUnloadFlows();
		default:
			return null;
		}
	}

	private List<Flow> getUnloadFlows() {
		// TODO Auto-generated method stub
		return null;
	}

}
