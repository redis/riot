package com.redis.riot.cli;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.InputStreamResource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redis.riot.file.resource.XmlItemReader;
import com.redis.riot.file.resource.XmlItemReaderBuilder;
import com.redis.riot.file.resource.XmlObjectReader;
import com.redis.spring.batch.common.KeyValue;
import com.redis.testcontainers.RedisStackContainer;

import picocli.CommandLine.ExitCode;
import picocli.CommandLine.ParseResult;

class StackToStackIntegrationTests extends AbstractIntegrationTests {

	private static final RedisStackContainer source = RedisContainerFactory.stack();
	private static final RedisStackContainer target = RedisContainerFactory.stack();

	@Override
	protected RedisStackContainer getRedisServer() {
		return source;
	}

	@Override
	protected RedisStackContainer getTargetRedisServer() {
		return target;
	}

	private static Path tempDir;

	@BeforeAll
	public void setupFiles() throws IOException {
		tempDir = Files.createTempDirectory(getClass().getName());
	}

	protected Path tempFile(String filename) throws IOException {
		Path path = tempDir.resolve(filename);
		if (Files.exists(path)) {
			Files.delete(path);
		}
		return path;
	}

	@SuppressWarnings("rawtypes")
	@Test
	void fileDumpImport(TestInfo info) throws Exception {
		List<KeyValue> records = exportToJsonFile(info);
		commands.flushall();
		execute(info, "dump-import", this::executeFileDumpImport);
		awaitUntil(() -> records.size() == Math.toIntExact(commands.dbsize()));
	}

	@SuppressWarnings("rawtypes")
	@Test
	void fileExportJSON(TestInfo info) throws Exception {
		List<KeyValue> records = exportToJsonFile(info);
		Assertions.assertEquals(commands.dbsize(), records.size());
	}

	@SuppressWarnings("rawtypes")
	private List<KeyValue> exportToJsonFile(TestInfo info) throws Exception {
		String filename = "file-export-json";
		Path file = tempFile("redis.json");
		generate(info, generator(73));
		execute(info, filename, r -> executeFileDumpExport(r, info));
		JsonItemReaderBuilder<KeyValue> builder = new JsonItemReaderBuilder<>();
		builder.name("json-data-structure-file-reader");
		builder.resource(new FileSystemResource(file));
		JacksonJsonObjectReader<KeyValue> objectReader = new JacksonJsonObjectReader<>(KeyValue.class);
		objectReader.setMapper(new ObjectMapper());
		builder.jsonObjectReader(objectReader);
		JsonItemReader<KeyValue> reader = builder.build();
		reader.open(new ExecutionContext());
		try {
			return readAll(reader);
		} finally {
			reader.close();
		}
	}

	private int executeFileDumpImport(ParseResult parseResult) {
		FileDumpImportCommand command = command(parseResult);
		command.args.files = command.args.files.stream().map(this::replace).collect(Collectors.toList());
		return ExitCode.OK;
	}

	private int executeFileDumpExport(ParseResult parseResult, TestInfo info) {
		FileDumpExportCommand command = command(parseResult);
		command.setName(name(info));
		command.args.file = replace(command.args.file);
		return ExitCode.OK;
	}

	private String replace(String file) {
		return file.replace("/tmp", tempDir.toString());
	}

	@SuppressWarnings("rawtypes")
	@Test
	@Disabled("Needs update")
	void fileExportJSONGz(TestInfo info) throws Exception {
		Path file = tempFile("beers.json.gz");
		execute(info, "file-import-json");
		execute(info, "file-export-json-gz", r -> executeFileDumpExport(r, info));
		JsonItemReaderBuilder<Map> builder = new JsonItemReaderBuilder<>();
		builder.name("json-file-reader");
		FileSystemResource resource = new FileSystemResource(file);
		builder.resource(
				new InputStreamResource(new GZIPInputStream(resource.getInputStream()), resource.getDescription()));
		JacksonJsonObjectReader<Map> objectReader = new JacksonJsonObjectReader<>(Map.class);
		objectReader.setMapper(new ObjectMapper());
		builder.jsonObjectReader(objectReader);
		JsonItemReader<Map> reader = builder.build();
		reader.open(new ExecutionContext());
		try {
			Assertions.assertEquals(keyCount("beer:*"), readAll(reader).size());
		} finally {
			reader.close();
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	void fileExportXml(TestInfo info) throws Exception {
		String filename = "file-export-xml";
		generate(info, generator(73));
		Path file = tempFile("redis.xml");
		execute(info, filename, r -> executeFileDumpExport(r, info));
		XmlItemReaderBuilder<KeyValue> builder = new XmlItemReaderBuilder<>();
		builder.name("xml-file-reader");
		builder.resource(new FileSystemResource(file));
		XmlObjectReader<KeyValue> xmlObjectReader = new XmlObjectReader<>(KeyValue.class);
		xmlObjectReader.setMapper(new XmlMapper());
		builder.xmlObjectReader(xmlObjectReader);
		XmlItemReader<KeyValue<String>> reader = (XmlItemReader) builder.build();
		reader.open(new ExecutionContext());
		List<KeyValue<String>> records = readAll(reader);
		reader.close();
		Assertions.assertEquals(commands.dbsize(), records.size());
		for (KeyValue<String> record : records) {
			String key = record.getKey();
			switch (record.getType()) {
			case HASH:
				Assertions.assertEquals(record.getValue(), commands.hgetall(key));
				break;
			case STRING:
				Assertions.assertEquals(record.getValue(), commands.get(key));
				break;
			default:
				break;
			}
		}
	}

}
