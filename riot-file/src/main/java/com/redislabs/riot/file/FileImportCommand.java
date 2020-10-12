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
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineCallbackHandler;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.separator.DefaultRecordSeparatorPolicy;
import org.springframework.batch.item.file.transform.AbstractLineTokenizer;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FixedLengthTokenizer;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.batch.item.redis.RedisKeyValueItemWriter;
import org.springframework.batch.item.redis.support.KeyValue;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.batch.item.xml.XmlItemReader;
import org.springframework.batch.item.xml.XmlObjectReader;
import org.springframework.batch.item.xml.support.XmlItemReaderBuilder;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redislabs.riot.AbstractImportCommand;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Slf4j
@SuppressWarnings("rawtypes")
@Command(name = "import", description = "Import file(s) into Redis")
public class FileImportCommand extends AbstractImportCommand {

	@Parameters(arity = "1..*", description = "One ore more files or URLs", paramLabel = "FILE")
	private String[] files;
	@Mixin
	protected FileOptions fileOptions = new FileOptions();
	@Option(names = "--fields", arity = "1..*", description = "Delimited/FW field names", paramLabel = "<names>")
	private String[] names;
	@Getter
	@Option(names = { "-h", "--header" }, description = "Delimited/FW first line contains field names")
	private boolean header;
	@Option(names = "--delimiter", description = "Delimiter character", paramLabel = "<string>")
	private String delimiter;
	@CommandLine.Option(names = "--skip", description = "Delimited/FW lines to skip at start", paramLabel = "<count>")
	private Integer linesToSkip;
	@CommandLine.Option(names = "--include", arity = "1..*", description = "Delimited/FW field indices to include (0-based)", paramLabel = "<index>")
	private int[] includedFields = new int[0];
	@CommandLine.Option(names = "--ranges", arity = "1..*", description = "Fixed-width column ranges", paramLabel = "<int>")
	private Range[] columnRanges = new Range[0];
	@CommandLine.Option(names = "--quote", description = "Escape character for delimited files (default: ${DEFAULT-VALUE})", paramLabel = "<char>")
	private Character quoteCharacter = DelimitedLineTokenizer.DEFAULT_QUOTE_CHARACTER;

	private String delimiter(String file) {
		if (delimiter == null) {
			String extension = FileOptions.extension(file);
			if (extension != null) {
				switch (extension) {
				case FileOptions.EXT_TSV:
					return DelimitedLineTokenizer.DELIMITER_TAB;
				case FileOptions.EXT_CSV:
					return DelimitedLineTokenizer.DELIMITER_COMMA;
				}
			}
			return DelimitedLineTokenizer.DELIMITER_COMMA;
		}
		return delimiter;
	}

	@Override
	protected List<ItemReader> readers() throws IOException {
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
		List<ItemReader> readers = new ArrayList<>(fileList.size());
		for (String file : fileList) {
			FileType fileType = fileOptions.fileType(file);
			Resource resource = fileOptions.inputResource(file);
			AbstractItemStreamItemReader reader = reader(file, fileType, resource);
			reader.setName(fileOptions.fileName(resource));
			readers.add(reader);
		}
		return readers;
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

	protected AbstractItemStreamItemReader reader(String file, FileType fileType, Resource resource)
			throws IOException {
		switch (fileType) {
		case DELIMITED:
			DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
			tokenizer.setDelimiter(delimiter(file));
			tokenizer.setQuoteCharacter(quoteCharacter);
			if (includedFields.length > 0) {
				tokenizer.setIncludedFields(includedFields);
			}
			return flatFileReader(resource, tokenizer);
		case FIXED:
			FixedLengthTokenizer fixedLengthTokenizer = new FixedLengthTokenizer();
			Assert.notEmpty(columnRanges, "Column ranges are required");
			fixedLengthTokenizer.setColumns(columnRanges);
			return flatFileReader(resource, fixedLengthTokenizer);
		case JSON:
			if (getRedisCommands().isEmpty()) {
				return jsonReader(resource, KeyValue.class);
			}
			return jsonReader(resource, Map.class);
		case XML:
			if (getRedisCommands().isEmpty()) {
				return xmlReader(resource, KeyValue.class);
			}
			return xmlReader(resource, Map.class);
		}
		throw new IllegalArgumentException("Unsupported file type: " + fileType);
	}

	private FlatFileItemReader<Map<String, Object>> flatFileReader(Resource resource, AbstractLineTokenizer tokenizer) {
		tokenizer.setNames(names == null ? new String[0] : names);
		return new FlatFileItemReaderBuilder<Map<String, Object>>().name("flat-file-reader").resource(resource)
				.encoding(fileOptions.getEncoding()).lineTokenizer(tokenizer).linesToSkip(linesToSkip()).strict(true)
				.saveState(false).fieldSetMapper(new MapFieldSetMapper())
				.recordSeparatorPolicy(new DefaultRecordSeparatorPolicy())
				.skippedLinesCallback(new HeaderCallbackHandler(tokenizer)).build();
	}

	private class HeaderCallbackHandler implements LineCallbackHandler {

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

	private int linesToSkip() {
		if (linesToSkip == null) {
			if (header) {
				return 1;
			}
			return 0;
		}
		return linesToSkip;
	}

	@Override
	protected ItemProcessor processor() {
		if (getRedisCommands().isEmpty()) {
			return (ItemProcessor) new JsonKeyValueItemProcessor();
		}
		return (ItemProcessor) mapProcessor();
	}

	@Override
	protected ItemWriter writer() throws Exception {
		if (getRedisCommands().isEmpty()) {
			RedisKeyValueItemWriter<String, String> writer = new RedisKeyValueItemWriter<String, String>();
			getApp().configure(writer);
			return writer;
		}
		return super.writer();
	}

}
