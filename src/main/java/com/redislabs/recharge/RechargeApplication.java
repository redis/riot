package com.redislabs.recharge;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.explore.support.SimpleJobExplorer;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean;
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
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
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
import com.redislabs.recharge.RechargeConfiguration.EntityConfiguration;
import com.redislabs.recharge.RechargeConfiguration.FileConfiguration;
import com.redislabs.recharge.RechargeConfiguration.FileType;
import com.redislabs.recharge.RechargeConfiguration.GeneratorConfiguration;
import com.redislabs.recharge.RechargeConfiguration.IndexConfiguration;
import com.redislabs.recharge.file.JsonItemReader;
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

	@Bean
	public ResourcelessTransactionManager transactionManager() {
		return new ResourcelessTransactionManager();
	}

	@Bean
	public MapJobRepositoryFactoryBean mapJobRepositoryFactory(ResourcelessTransactionManager transactionManager)
			throws Exception {
		MapJobRepositoryFactoryBean factory = new MapJobRepositoryFactoryBean(transactionManager);
		factory.afterPropertiesSet();
		return factory;
	}

	@Bean
	public JobRepository jobRepository(MapJobRepositoryFactoryBean repositoryFactory) throws Exception {
		return repositoryFactory.getObject();
	}

	@Bean
	public JobExplorer jobExplorer(MapJobRepositoryFactoryBean repositoryFactory) {
		return new SimpleJobExplorer(repositoryFactory.getJobInstanceDao(), repositoryFactory.getJobExecutionDao(),
				repositoryFactory.getStepExecutionDao(), repositoryFactory.getExecutionContextDao());
	}

	@Bean
	public SimpleJobLauncher jobLauncher(JobRepository jobRepository) {
		SimpleJobLauncher launcher = new SimpleJobLauncher();
		launcher.setJobRepository(jobRepository);
		return launcher;
	}

	@Autowired
	private JobLauncher jobLauncher;
	@Autowired
	private JobBuilderFactory jobBuilderFactory;
	@Autowired
	private StepBuilderFactory stepFactory;
	@Autowired
	private RechargeConfiguration rechargeConfig;
	@Autowired
	private RedisProperties redisConfig;
	@Autowired
	private StringRedisTemplate redisTemplate;

	private List<Flow> getLoadFlows() throws Exception {
		List<Flow> flows = new ArrayList<>();
		for (Entry<String, EntityConfiguration> entity : rechargeConfig.getEntities().entrySet()) {
			AbstractItemCountingItemStreamItemReader<Map<String, Object>> reader = getReader(entity);
			reader.setName(entity.getKey() + "-load-reader");
			sanitize(entity.getValue());
			SimpleStepBuilder<Map<String, Object>, Map<String, Object>> step = stepFactory
					.get(entity.getKey() + "-load-step")
					.<Map<String, Object>, Map<String, Object>>chunk(rechargeConfig.getChunkSize());
			if (rechargeConfig.getMaxItemCount() != -1) {
				reader.setMaxItemCount(rechargeConfig.getMaxItemCount());
			}
			step.reader(reader);
			step.writer(getWriter(entity));
			step.throttleLimit(rechargeConfig.getMaxThreads());
			flows.add(new FlowBuilder<Flow>(entity.getKey() + "-load-flow").from(step.build()).end());
		}
		return flows;
	}

	private void sanitize(EntityConfiguration config) {
		if (config.getKeys() == null || config.getKeys().length == 0) {
			config.setKeys(config.getFields());
		}
		for (IndexConfiguration indexConfig : config.getIndexes()) {
			if (indexConfig.getScore() == null) {
				indexConfig.setScore(config.getKeys()[0]);
			}
		}
	}

	private ItemWriter<Map<String, Object>> getWriter(Entry<String, EntityConfiguration> entity) {
		ItemWriter<Map<String, Object>> writer = getEntityWriter(entity);
		if (entity.getValue().getIndexes().size() > 0) {
			List<ItemWriter<? super Map<String, Object>>> writers = new ArrayList<>();
			writers.add(writer);
			for (IndexConfiguration indexConfig : entity.getValue().getIndexes()) {
				writers.add(getIndexWriter(entity, indexConfig));
			}
			CompositeItemWriter<Map<String, Object>> composite = new CompositeItemWriter<>();
			composite.setDelegates(writers);
			return composite;
		}
		return writer;
	}

	private AbstractIndexWriter getIndexWriter(Entry<String, EntityConfiguration> entity,
			IndexConfiguration indexConfig) {
		switch (indexConfig.getType()) {
		case Geo:
			return new GeoIndexWriter(redisTemplate, entity, indexConfig);
		case List:
			return new ListIndexWriter(redisTemplate, entity, indexConfig);
		case Search:
			return new RediSearchIndexWriter(redisTemplate, entity, indexConfig, redisConfig);
		case Zset:
			return new ZSetIndexWriter(redisTemplate, entity, indexConfig);
		default:
			return new SetIndexWriter(redisTemplate, entity, indexConfig);
		}
	}

	private AbstractItemCountingItemStreamItemReader<Map<String, Object>> getReader(
			Entry<String, EntityConfiguration> entity) throws Exception {
		if (entity.getValue().getGenerator() != null) {
			GeneratorConfiguration generator = entity.getValue().getGenerator();
			if (entity.getValue().getFields() == null) {
				entity.getValue().setFields(generator.getFields().keySet().toArray(new String[0]));
			}
			return new GeneratorEntityItemReader(generator);
		}
		if (entity.getValue().getFile() != null) {
			FileConfiguration fileConfig = entity.getValue().getFile();
			if (fileConfig.getType() == null) {
				fileConfig.setType(guessFileType(fileConfig.getPath()));
			}
			switch (fileConfig.getType()) {
			case Delimited:
				return getDelimitedReader(entity);
			case FixedLength:
				return getFixedLengthReader(entity);
			case Json:
				return new JsonItemReader();
			default:
				throw new RechargeException("No reader found for file " + fileConfig.getPath());
			}
		}
		throw new RechargeException("No entity type configured");
	}

	private FlatFileItemReaderBuilder<Map<String, Object>> getFlatFileReaderBuilder(String entityName,
			FileConfiguration fileConfig) throws IOException {
		FlatFileItemReaderBuilder<Map<String, Object>> builder = new FlatFileItemReaderBuilder<>();
		Resource resource = getResource(fileConfig);
		builder.resource(resource);
		if (fileConfig.getEncoding() != null) {
			builder.encoding(fileConfig.getEncoding());
		}
		builder.strict(true);
		builder.saveState(false);
		builder.fieldSetMapper(new MapFieldSetMapper());
		if (fileConfig.getLinesToSkip() != null) {
			builder.linesToSkip(fileConfig.getLinesToSkip());
		}
		return builder;
	}

	private FlatFileItemReader<Map<String, Object>> getDelimitedReader(Entry<String, EntityConfiguration> entity)
			throws IOException {
		FileConfiguration config = entity.getValue().getFile();
		FlatFileItemReaderBuilder<Map<String, Object>> builder = getFlatFileReaderBuilder(entity.getKey(), config);
		DelimitedBuilder<Map<String, Object>> delimitedBuilder = builder.delimited();
		if (config.getDelimiter() != null) {
			delimitedBuilder.delimiter(config.getDelimiter());
		}
		if (config.getIncludedFields() != null) {
			delimitedBuilder.includedFields(config.getIncludedFields());
		}
		if (config.getQuoteCharacter() != null) {
			delimitedBuilder.quoteCharacter(config.getQuoteCharacter());
		}
		delimitedBuilder.names(entity.getValue().getFields());
		return builder.build();
	}

	private FlatFileItemReader<Map<String, Object>> getFixedLengthReader(Entry<String, EntityConfiguration> entity)
			throws IOException {
		FileConfiguration config = entity.getValue().getFile();
		FlatFileItemReaderBuilder<Map<String, Object>> builder = getFlatFileReaderBuilder(entity.getKey(), config);
		FixedLengthBuilder<Map<String, Object>> fixedLengthBuilder = builder.fixedLength();
		if (config.getRanges() != null) {
			fixedLengthBuilder.columns(getRanges(config.getRanges()));
		}
		if (config.getStrict() != null) {
			fixedLengthBuilder.strict(config.getStrict());
		}
		fixedLengthBuilder.names(entity.getValue().getFields());
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

	private FileType guessFileType(String path) {
		String extension = getFilenameGroup(path, FILE_EXTENSION);
		if (extension == null) {
			log.warn("Could not determine file type from path {}", path);
		}
		log.debug("Found file extension '{}' for path {}", extension, path);
		return rechargeConfig.getFileTypes().getOrDefault(extension, FileType.Delimited);
	}

	private Pattern filePathPattern = Pattern
			.compile("(?<" + FILE_BASENAME + ">.+)\\.(?<" + FILE_EXTENSION + ">\\w+)(?<" + FILE_GZ + ">\\.gz)?");

	private boolean isGzip(FileConfiguration config) {
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

	private Resource getResource(FileConfiguration config) throws IOException {
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

	private AbstractItemStreamItemWriter<Map<String, Object>> getEntityWriter(
			Entry<String, EntityConfiguration> entity) {
		switch (entity.getValue().getType()) {
		case Nil:
			return new NilWriter(redisTemplate, entity);
		case String:
			return new StringWriter(redisTemplate, entity, getObjectWriter(entity));
		default:
			return new HashWriter(redisTemplate, entity);
		}
	}

	private ObjectWriter getObjectWriter(Entry<String, EntityConfiguration> entity) {
		switch (entity.getValue().getFormat()) {
		case Xml:
			return new XmlMapper().writer().withRootName(entity.getKey());
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
			switch (command) {
			case "help":
				printHelp();
				break;
			case "load":
				run(command, getLoadFlows());
				break;
			case "unload":
				run(command, getUnloadFlows());
				break;
			}
		} else {
			log.error("No command given. Run 'recharge help' for usage");
		}
	}

	private void printHelp() {

	}

	private void run(String command, List<Flow> flows) throws JobExecutionAlreadyRunningException, JobRestartException,
			JobInstanceAlreadyCompleteException, JobParametersInvalidException {
		SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor();
		executor.setConcurrencyLimit(rechargeConfig.getMaxThreads());
		Flow flow = new FlowBuilder<Flow>("split-flow").split(executor).add(flows.toArray(new Flow[flows.size()]))
				.build();
		Job job = jobBuilderFactory.get(command + "-job").start(flow).end().build();
		jobLauncher.run(job, new JobParameters());
	}

	private List<Flow> getUnloadFlows() {
		// TODO Auto-generated method stub
		return null;
	}

}
