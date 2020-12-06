package com.redislabs.riot.file;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineCallbackHandler;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.separator.DefaultRecordSeparatorPolicy;
import org.springframework.batch.item.file.transform.AbstractLineTokenizer;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FixedLengthTokenizer;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.batch.item.redis.DataStructureItemWriter;
import org.springframework.batch.item.redis.support.DataStructure;
import org.springframework.batch.item.redis.support.Transfer;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.batch.item.xml.XmlItemReader;
import org.springframework.batch.item.xml.XmlObjectReader;
import org.springframework.batch.item.xml.support.XmlItemReaderBuilder;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redislabs.riot.AbstractMapImportCommand;
import com.redislabs.riot.RedisOptions;

import io.lettuce.core.AbstractRedisClient;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;

@Slf4j
@Command(name = "import", description = "Import file(s) into Redis")
public class FileImportCommand extends AbstractMapImportCommand<Object, Object> {

	@Parameters(arity = "1..*", description = "One ore more files or URLs", paramLabel = "FILE")
	private String[] files;
	@Mixin
	protected FileOptions fileOptions = new FileOptions();
	@ArgGroup(exclusive = false, heading = "Flat file options%n")
	protected FlatFileOptions flatFileOptions = new FlatFileOptions();

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected List<Transfer<Object, Object>> transfers(RedisOptions redisOptions, AbstractRedisClient client)
			throws Exception {
		List<String> fileList = new ArrayList<>();
		for (String file : files) {
			if (fileOptions.isFile(file)) {
				Path path = Paths.get(file);
				if (Files.exists(path)) {
					fileList.add(file);
				} else {
					// Path might be glob pattern
					Path parent = path.getParent();
					try (DirectoryStream<Path> stream = Files.newDirectoryStream(parent,
							path.getFileName().toString())) {
						stream.forEach(p -> fileList.add(p.toString()));
					} catch (IOException e) {
						log.debug("Could not list files in {}", path, e);
					}
				}
			} else {
				fileList.add(file);
			}
		}
		List<Transfer<Object, Object>> transfers = new ArrayList<>(fileList.size());
		ItemProcessor<Object, Object> processor = getRedisCommands().isEmpty()
				? (ItemProcessor) new JsonDataStructureItemProcessor()
				: (ItemProcessor) mapProcessor(client);
		for (String file : fileList) {
			FileType fileType = fileOptions.fileType(file);
			Resource resource = fileOptions.inputResource(file);
			AbstractItemStreamItemReader<Object> reader = reader(file, fileType, resource);
			reader.setName(fileOptions.filename(resource));
			transfers.add(transfer(reader, processor, writer(client, redisOptions)));
		}
		return transfers;
	}

	protected <T> JsonItemReader<T> jsonReader(Resource resource, Class<T> clazz) {
		JsonItemReaderBuilder<T> jsonReaderBuilder = new JsonItemReaderBuilder<>();
		jsonReaderBuilder.name("json-file-reader");
		jsonReaderBuilder.resource(resource);
		JacksonJsonObjectReader<T> jsonObjectReader = new JacksonJsonObjectReader<>(clazz);
		jsonObjectReader.setMapper(new ObjectMapper());
		jsonReaderBuilder.jsonObjectReader(jsonObjectReader);
		return jsonReaderBuilder.build();
	}

	protected <T> XmlItemReader<T> xmlReader(Resource resource, Class<T> clazz) {
		XmlItemReaderBuilder<T> xmlReaderBuilder = new XmlItemReaderBuilder<>();
		xmlReaderBuilder.name("xml-file-reader");
		xmlReaderBuilder.resource(resource);
		XmlObjectReader<T> xmlObjectReader = new XmlObjectReader<>(clazz);
		xmlObjectReader.setMapper(new XmlMapper());
		xmlReaderBuilder.xmlObjectReader(xmlObjectReader);
		return xmlReaderBuilder.build();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected AbstractItemStreamItemReader<Object> reader(String file, FileType fileType, Resource resource)
			throws IOException {
		switch (fileType) {
		case DELIMITED:
			DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
			tokenizer.setDelimiter(flatFileOptions.delimiter(file));
			tokenizer.setQuoteCharacter(flatFileOptions.getQuoteCharacter());
			if (flatFileOptions.getIncludedFields().length > 0) {
				tokenizer.setIncludedFields(flatFileOptions.getIncludedFields());
			}
			return (FlatFileItemReader) flatFileReader(resource, tokenizer);
		case FIXED:
			FixedLengthTokenizer fixedLengthTokenizer = new FixedLengthTokenizer();
			Assert.notEmpty(flatFileOptions.getColumnRanges(), "Column ranges are required");
			fixedLengthTokenizer.setColumns(flatFileOptions.getColumnRanges());
			return (FlatFileItemReader) flatFileReader(resource, fixedLengthTokenizer);
		case JSON:
			if (getRedisCommands().isEmpty()) {
				return (JsonItemReader) jsonReader(resource, DataStructure.class);
			}
			return (JsonItemReader) jsonReader(resource, Map.class);
		case XML:
			if (getRedisCommands().isEmpty()) {
				return (XmlItemReader) xmlReader(resource, DataStructure.class);
			}
			return (XmlItemReader) xmlReader(resource, Map.class);
		}
		throw new IllegalArgumentException("Unsupported file type: " + fileType);
	}

	private FlatFileItemReader<Map<String, Object>> flatFileReader(Resource resource, AbstractLineTokenizer tokenizer) {
		tokenizer.setNames(flatFileOptions.getNames() == null ? new String[0] : flatFileOptions.getNames());
		return new FlatFileItemReaderBuilder<Map<String, Object>>().name("flat-file-reader").resource(resource)
				.encoding(fileOptions.getEncoding()).lineTokenizer(tokenizer)
				.linesToSkip(flatFileOptions.getLinesToSkip()).strict(true).saveState(false)
				.fieldSetMapper(new MapFieldSetMapper()).recordSeparatorPolicy(new DefaultRecordSeparatorPolicy())
				.skippedLinesCallback(new HeaderCallbackHandler(tokenizer)).build();
	}

	private static class HeaderCallbackHandler implements LineCallbackHandler {

		private AbstractLineTokenizer tokenizer;

		public HeaderCallbackHandler(AbstractLineTokenizer tokenizer) {
			this.tokenizer = tokenizer;
		}

		@Override
		public void handleLine(String line) {
			log.debug("Found header {}", line);
			tokenizer.setNames(tokenizer.tokenize(line).getValues());
		}
	}

	@Override
	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected ItemWriter<Object> writer(AbstractRedisClient client, RedisOptions redisOptions) throws Exception {
		if (getRedisCommands().isEmpty()) {
			return (ItemWriter) DataStructureItemWriter.builder().client(client).poolConfig(redisOptions.poolConfig())
					.build();
		}
		return super.writer(client, redisOptions);
	}

}
