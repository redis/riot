package com.redislabs.recharge;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder.DelimitedBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder.FixedLengthBuilder;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.util.ResourceUtils;

import com.redislabs.recharge.config.Delimited;
import com.redislabs.recharge.config.File;
import com.redislabs.recharge.config.FileType;
import com.redislabs.recharge.config.FixedLength;
import com.redislabs.recharge.config.FlatFile;
import com.redislabs.recharge.config.Recharge;
import com.redislabs.recharge.flatfile.MapFieldSetMapper;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

	private Logger log = LoggerFactory.getLogger(BatchConfiguration.class);

	private Pattern fileExtensionPattern = Pattern.compile(".*\\.(?<filetype>\\w+)(?<gz>\\.gz)?");

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	private Recharge config;

	@Autowired
	private RedisItemWriter writer;

	@Bean
	public ItemReader<Map<String, String>> reader() throws IOException {
		ItemReader<Map<String, String>> reader = null;
		if (config.getFile().getPath() != null) {
			Matcher matcher = fileExtensionPattern.matcher(config.getFile().getPath());
			FileType type = config.getFile().getType();
			Boolean gzip = config.getFile().getGzip();
			if (matcher.find()) {
				if (type == null) {
					type = getFiletype(matcher.group("filetype"));
				}
				if (gzip == null) {
					String gz = matcher.group("gz");
					if (gz != null && gz.length() > 0) {
						gzip = true;
					}
				}
			}
			if (type == null) {
				log.info("Could not determine type of file {}; try specifying the type with --file.type=blah",
						config.getFile().getPath());
				return null;
			}
			Resource resource = getResource(config.getFile().getPath());
			if (Boolean.TRUE.equals(gzip)) {
				resource = new GZIPResource(resource);
			}
			if (config.getRedis().getKeyPrefix() == null) {
				config.getRedis().setKeyPrefix(getBaseFilename(resource));
			}
			setRedisKeyFields(config.getFile().getFlat());
			switch (type) {
			case FixedLength:
				return getFixedLengthItemReader(resource, config.getFile());
			default:
				return getDelimitedItemReader(resource, config.getFile());
			}
		}
		return reader;
	}

	private Resource getResource(String path) throws MalformedURLException {
		if (ResourceUtils.isUrl(path)) {
			return new UrlResource(path);
		}
		return new FileSystemResource(path);
	}

	private void setRedisKeyFields(FlatFile flatFile) {
		if (config.getRedis().getKeyFields() == null) {
			config.getRedis().setKeyFields(new String[] { flatFile.getFieldNames()[0] });
		}
	}

	private String getBaseFilename(Resource resource) {
		int pos = resource.getFilename().indexOf(".");
		if (pos == -1) {
			return resource.getFilename();
		}
		return resource.getFilename().substring(0, pos);
	}

	private FileType getFiletype(String extension) {
		switch (extension) {
		case "fw":
			return FileType.FixedLength;
		case "csv":
			return FileType.Delimited;
		case "json":
			return FileType.JSON;
		case "xml":
			return FileType.XML;
		default:
			return FileType.Delimited;
		}
	}

	private ItemReader<Map<String, String>> getDelimitedItemReader(Resource resource, File file) throws IOException {
		FlatFileItemReaderBuilder<Map<String, String>> readerBuilder = getReaderBuilder(resource, file, file.getFlat());
		Delimited delimited = file.getFlat().getDelimited();
		DelimitedBuilder<Map<String, String>> delimitedBuilder = readerBuilder.delimited();
		if (delimited.getDelimiter() != null) {
			delimitedBuilder.delimiter(delimited.getDelimiter());
		}
		if (delimited.getIncludedFields() != null) {
			delimitedBuilder.includedFields(delimited.getIncludedFields());
		}
		if (delimited.getQuoteCharacter() != null) {
			delimitedBuilder.quoteCharacter(delimited.getQuoteCharacter());
		}
		if (file.getFlat().getFieldNames() != null) {
			delimitedBuilder.names(file.getFlat().getFieldNames());
		}
		return readerBuilder.build();
	}

	private ItemReader<Map<String, String>> getFixedLengthItemReader(Resource resource, File file) throws IOException {
		FlatFileItemReaderBuilder<Map<String, String>> readerBuilder = getReaderBuilder(resource, file, file.getFlat());
		FixedLength fixedLength = file.getFlat().getFixedLength();
		FixedLengthBuilder<Map<String, String>> fixedLengthBuilder = readerBuilder.fixedLength();
		if (fixedLength.getRanges() != null) {
			fixedLengthBuilder.columns(getRanges(fixedLength.getRanges()));
		}
		if (file.getFlat().getFieldNames() != null) {
			fixedLengthBuilder.names(file.getFlat().getFieldNames());
		}
		if (fixedLength.getStrict() != null) {
			fixedLengthBuilder.strict(fixedLength.getStrict());
		}
		return readerBuilder.build();
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

	private FlatFileItemReaderBuilder<Map<String, String>> getReaderBuilder(Resource resource, File file,
			FlatFile flatFile) throws IOException {
		FlatFileItemReaderBuilder<Map<String, String>> builder = new FlatFileItemReaderBuilder<Map<String, String>>();
		builder.resource(resource);
		if (flatFile.getLinesToSkip() != null) {
			builder.linesToSkip(flatFile.getLinesToSkip());
		}
		if (file.getEncoding() != null) {
			builder.encoding(file.getEncoding());
		}
		if (flatFile.getMaxItemCount() != null) {
			builder.maxItemCount(flatFile.getMaxItemCount());
		}
		builder.strict(true);
		builder.saveState(false);
		builder.fieldSetMapper(new MapFieldSetMapper());
		return builder;
	}

	@Bean
	public Job releaseLoadJob(Step releaseLoadStep) {
		return jobBuilderFactory.get("releaseLoadJob").incrementer(new RunIdIncrementer()).flow(releaseLoadStep).end()
				.build();
	}

	@Bean
	public Step releaseLoadStep() throws IOException {
		SimpleStepBuilder<Map<String, String>, Map<String, String>> builder = stepBuilderFactory.get("releaseLoadStep")
				.<Map<String, String>, Map<String, String>>chunk(config.getBatchSize()).reader(reader()).writer(writer);
		if (config.getMaxThreads() != null) {
			SimpleAsyncTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();
			taskExecutor.setConcurrencyLimit(config.getMaxThreads());
			builder.taskExecutor(taskExecutor);
		}
		return builder.build();
	}
}
