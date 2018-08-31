package com.redislabs.recharge;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
import org.springframework.batch.item.file.BufferedReaderFactory;
import org.springframework.batch.item.file.DefaultBufferedReaderFactory;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder.DelimitedBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder.FixedLengthBuilder;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
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
import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.recharge.RechargeConfiguration.EntityConfiguration;
import com.redislabs.recharge.RechargeConfiguration.FileConfiguration;
import com.redislabs.recharge.RechargeConfiguration.FileType;
import com.redislabs.recharge.RechargeConfiguration.IndexConfiguration;
import com.redislabs.recharge.file.JsonItemReader;
import com.redislabs.recharge.generator.GeneratorEntityItemReader;
import com.redislabs.recharge.redis.AbstractRedisWriter;
import com.redislabs.recharge.redis.HashWriter;
import com.redislabs.recharge.redis.NilWriter;
import com.redislabs.recharge.redis.StringWriter;
import com.redislabs.recharge.redis.index.AbstractIndexWriter;
import com.redislabs.recharge.redis.index.GeoIndexWriter;
import com.redislabs.recharge.redis.index.ListIndexWriter;
import com.redislabs.recharge.redis.index.SearchIndexWriter;
import com.redislabs.recharge.redis.index.SetIndexWriter;
import com.redislabs.recharge.redis.index.SuggestionIndexWriter;
import com.redislabs.recharge.redis.index.ZSetIndexWriter;

import lombok.extern.slf4j.Slf4j;

@SpringBootApplication
@EnableBatchProcessing
@EnableAutoConfiguration
@Slf4j
public class RechargeApplication implements ApplicationRunner {

	private static final String FILE_BASENAME = "basename";
	private static final String FILE_EXTENSION = "extension";
	private static final String FILE_GZ = "gz";

	@Autowired
	private JobLauncher jobLauncher;
	@Autowired
	private JobBuilderFactory jobBuilderFactory;
	@Autowired
	private StepBuilderFactory stepFactory;
	@Autowired
	private RechargeConfiguration rechargeConfig;
	@Autowired
	private StringRedisTemplate redisTemplate;
	@Autowired
	private RediSearchClient rediSearchClient;

	private BufferedReaderFactory bufferedReaderFactory = new DefaultBufferedReaderFactory();

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

	private List<Flow> getLoadFlows() throws Exception {
		if (rechargeConfig.isFlushall()) {
			log.warn("***** FLUSHALL *****");
			redisTemplate.getConnectionFactory().getConnection().flushAll();
		}
		List<Flow> flows = new ArrayList<>();
		for (EntityConfiguration entityConfig : rechargeConfig.getEntities()) {
			AbstractItemCountingItemStreamItemReader<Map<String, Object>> reader = getReader(entityConfig);
			reader.setName(entityConfig.getName() + "-load-reader");
			if (entityConfig.getName() == null) {
				entityConfig.setName("entity" + rechargeConfig.getEntities().indexOf(entityConfig));
			}
			if (entityConfig.getKeys() == null || entityConfig.getKeys().length == 0) {
				entityConfig.setKeys(entityConfig.getFields());
			}
			SimpleStepBuilder<Map<String, Object>, Map<String, Object>> step = stepFactory
					.get(entityConfig.getName() + "-load-step")
					.<Map<String, Object>, Map<String, Object>>chunk(rechargeConfig.getChunkSize());
			if (entityConfig.getMaxItemCount() != -1) {
				reader.setMaxItemCount(entityConfig.getMaxItemCount());
			}
			step.reader(reader);
			step.writer(getWriter(entityConfig));
			step.throttleLimit(entityConfig.getMaxThreads());
			flows.add(new FlowBuilder<Flow>(entityConfig.getName() + "-load-flow").from(step.build()).end());
		}
		return flows;
	}

	private ItemWriter<Map<String, Object>> getWriter(EntityConfiguration entity) {
		ItemWriter<Map<String, Object>> writer = getEntityWriter(entity);
		if (entity.getIndexes().size() > 0) {
			List<ItemWriter<? super Map<String, Object>>> writers = new ArrayList<>();
			writers.add(writer);
			for (IndexConfiguration index : entity.getIndexes()) {
				if (index.getName() == null) {
					index.setName(getKeyspace(entity, index));
				}
//				if (index.getKeys() == null || index.getKeys().length == 0) {
//					index.setKeys(entity.getKeys());
//				}
				if (index.getFields() == null || index.getFields().length == 0) {
					index.setFields(entity.getKeys());
				}
				writers.add(getIndexWriter(entity, index));
			}
			CompositeItemWriter<Map<String, Object>> composite = new CompositeItemWriter<>();
			composite.setDelegates(writers);
			return composite;
		}
		return writer;
	}

	private String getKeyspace(EntityConfiguration entity, IndexConfiguration index) {
		String suffix = getSuffix(entity, index);
		switch (index.getType()) {
		case Search:
			return entity.getName() + "Idx" + suffix;
		case Suggestion:
			return entity.getName() + "Suggestion" + suffix;
		default:
			return String.join(AbstractRedisWriter.KEY_SEPARATOR, entity.getName(), "index" + suffix);
		}
	}

	private String getSuffix(EntityConfiguration entity, IndexConfiguration index) {
		if (entity.getIndexes().size() == 0) {
			return "";
		}
		return String.valueOf(entity.getIndexes().indexOf(index) + 1);
	}

	private AbstractIndexWriter getIndexWriter(EntityConfiguration entity, IndexConfiguration index) {
		switch (index.getType()) {
		case Geo:
			return new GeoIndexWriter(redisTemplate, entity, index);
		case List:
			return new ListIndexWriter(redisTemplate, entity, index);
		case Search:
			return new SearchIndexWriter(redisTemplate, entity, index, rediSearchClient);
		case Suggestion:
			return new SuggestionIndexWriter(redisTemplate, entity, index, rediSearchClient);
		case Zset:
			return new ZSetIndexWriter(redisTemplate, entity, index);
		default:
			return new SetIndexWriter(redisTemplate, entity, index);
		}
	}

	private AbstractItemCountingItemStreamItemReader<Map<String, Object>> getReader(EntityConfiguration entity)
			throws Exception {
		if (entity.getGenerator() != null && entity.getGenerator().size() > 0) {
			Map<String, String> generatorFields = entity.getGenerator();
			if (entity.getFields() == null) {
				entity.setFields(generatorFields.keySet().toArray(new String[0]));
			}
			return new GeneratorEntityItemReader(redisTemplate, entity.getFakerLocale(), generatorFields);
		}
		if (entity.getFile() != null) {
			FileConfiguration fileConfig = entity.getFileConfig();
			if (fileConfig.getType() == null) {
				fileConfig.setType(guessFileType(entity.getFile()));
			}
			switch (fileConfig.getType()) {
			case Delimited:
				return getDelimitedReader(entity);
			case FixedLength:
				return getFixedLengthReader(entity);
			case Json:
				return new JsonItemReader();
			default:
				throw new RechargeException("No reader found for file " + entity.getFile());
			}
		}
		throw new RechargeException("No entity type configured");
	}

	private FlatFileItemReaderBuilder<Map<String, Object>> getFlatFileReaderBuilder(EntityConfiguration entity)
			throws IOException {
		FlatFileItemReaderBuilder<Map<String, Object>> builder = new FlatFileItemReaderBuilder<>();
		Resource resource = getResource(entity);
		builder.resource(resource);
		if (entity.getName() == null) {
			entity.setName(getFileBaseName(resource));
		}
		FileConfiguration fileConfig = entity.getFileConfig();
		if (fileConfig.getEncoding() != null) {
			builder.encoding(fileConfig.getEncoding());
		}
		builder.strict(true);
		builder.saveState(false);
		builder.fieldSetMapper(new MapFieldSetMapper());
		builder.linesToSkip(fileConfig.getLinesToSkip());
		if (fileConfig.isHeader() && fileConfig.getLinesToSkip() == 0) {
			builder.linesToSkip(1);
		}
		return builder;
	}

	private String getFileBaseName(Resource resource) {
		String filename = resource.getFilename();
		int extensionIndex = filename.lastIndexOf(".");
		if (extensionIndex == -1) {
			return filename;
		}
		return filename.substring(0, extensionIndex);
	}

	private FlatFileItemReader<Map<String, Object>> getDelimitedReader(EntityConfiguration entity) throws IOException {
		FileConfiguration config = entity.getFileConfig();
		FlatFileItemReaderBuilder<Map<String, Object>> builder = getFlatFileReaderBuilder(entity);
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
		String[] fieldNames = entity.getFields();
		if (config.isHeader()) {
			Resource resource = getResource(entity);
			try {
				BufferedReader reader = bufferedReaderFactory.create(resource, config.getEncoding());
				DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
				if (config.getDelimiter() != null) {
					tokenizer.setDelimiter(config.getDelimiter());
				}
				if (config.getQuoteCharacter() != null) {
					tokenizer.setQuoteCharacter(config.getQuoteCharacter());
				}
				String line = reader.readLine();
				FieldSet fields = tokenizer.tokenize(line);
				fieldNames = fields.getValues();
				log.info("Found header {}", Arrays.toString(fieldNames));
			} catch (Exception e) {
				log.error("Could not read header for file {}", entity.getFile(), e);
			}
		}
		delimitedBuilder.names(fieldNames);
		return builder.build();
	}

	private FlatFileItemReader<Map<String, Object>> getFixedLengthReader(EntityConfiguration entity)
			throws IOException {
		FileConfiguration config = entity.getFileConfig();
		FlatFileItemReaderBuilder<Map<String, Object>> builder = getFlatFileReaderBuilder(entity);
		FixedLengthBuilder<Map<String, Object>> fixedLengthBuilder = builder.fixedLength();
		if (config.getRanges() != null) {
			fixedLengthBuilder.columns(getRanges(config.getRanges()));
		}
		if (config.getStrict() != null) {
			fixedLengthBuilder.strict(config.getStrict());
		}
		fixedLengthBuilder.names(entity.getFields());
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

	private boolean isGzip(EntityConfiguration entity) {
		if (entity.getFileConfig().getGzip() == null) {
			String gz = getFilenameGroup(entity.getFile(), FILE_GZ);
			return gz != null && gz.length() > 0;
		}
		return entity.getFileConfig().getGzip();
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

	private Resource getResource(EntityConfiguration entity) throws IOException {
		Resource resource = getResource(entity.getFile());
		if (isGzip(entity)) {
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

	private AbstractItemStreamItemWriter<Map<String, Object>> getEntityWriter(EntityConfiguration entity) {
		switch (entity.getType()) {
		case Nil:
			return new NilWriter(redisTemplate, entity);
		case String:
			return new StringWriter(redisTemplate, entity, getObjectWriter(entity));
		default:
			return new HashWriter(redisTemplate, entity);
		}
	}

	private ObjectWriter getObjectWriter(EntityConfiguration entity) {
		switch (entity.getFormat()) {
		case Xml:
			return new XmlMapper().writer().withRootName(entity.getName());
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
				System.out.println("Unload command not yet supported");
				break;
			}
		} else {
			log.error("No command given. Run 'recharge help' for usage");
		}
	}

	private void printHelp() {
		// TODO
	}

	private void run(String command, List<Flow> flows) throws JobExecutionAlreadyRunningException, JobRestartException,
			JobInstanceAlreadyCompleteException, JobParametersInvalidException {
		SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor();
		if (rechargeConfig.isConcurrent()) {
			executor.setConcurrencyLimit(flows.size());
		}
		Flow flow = new FlowBuilder<Flow>("split-flow").split(executor).add(flows.toArray(new Flow[flows.size()]))
				.build();
		Job job = jobBuilderFactory.get(command + "-job").start(flow).end().build();
		jobLauncher.run(job, new JobParameters());
	}

}
