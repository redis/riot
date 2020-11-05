package com.redislabs.riot.file;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.batch.item.redis.support.DataStructure;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.InputStreamResource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.redislabs.riot.test.DataPopulator;

@SuppressWarnings("rawtypes")
public class TestJson extends AbstractFileTest {

    @Test
    public void exportRedis() throws Exception {
	List<DataStructure> records = exportToList();
	Assertions.assertEquals(commands().dbsize(), records.size());
    }

    private List<DataStructure> exportToList() throws Exception {
	Path file = tempFile("redis.json");
	DataPopulator.builder().connection(connection()).build().run();
	executeFile("/json/export.txt");
	JsonItemReaderBuilder<DataStructure> builder = new JsonItemReaderBuilder<>();
	builder.name("json-data-structure-file-reader");
	builder.resource(new FileSystemResource(file));
	JacksonJsonObjectReader<DataStructure> objectReader = new JacksonJsonObjectReader<>(DataStructure.class);
	objectReader.setMapper(new ObjectMapper());
	builder.jsonObjectReader(objectReader);
	JsonItemReader<DataStructure> reader = builder.build();
	return readAll(reader);
    }

    @Test
    public void importDataStructures() throws Exception {
	List<DataStructure> records = exportToList();
	commands().flushall();
	executeFile("/json/import.txt");
	Assertions.assertEquals(records.size(), commands().dbsize());
    }

    @Test
    public void exportGzip() throws Exception {
	Path file = tempFile("beers.json.gz");
	executeFile("/json/import-hmset.txt");
	executeFile("/json/export-gzip.txt");
	JsonItemReaderBuilder<Map> builder = new JsonItemReaderBuilder<>();
	builder.name("json-file-reader");
	FileSystemResource resource = new FileSystemResource(file);
	builder.resource(
		new InputStreamResource(new GZIPInputStream(resource.getInputStream()), resource.getDescription()));
	JacksonJsonObjectReader<Map> objectReader = new JacksonJsonObjectReader<>(Map.class);
	objectReader.setMapper(new ObjectMapper());
	builder.jsonObjectReader(objectReader);
	JsonItemReader<Map> reader = builder.build();
	List<Map> records = readAll(reader);
	Assertions.assertEquals(commands().keys("beer:*").size(), records.size());
    }

    @Test
    public void importElastic() throws Exception {
	executeFile("/json/import-elastic.txt");
	Assertions.assertEquals(2, commands().keys("estest:*").size());
	Map<String, String> doc1 = commands().hgetall("estest:doc1");
	Assertions.assertEquals("ruan", doc1.get("_source.name"));
	Assertions.assertEquals("3", doc1.get("_source.articles[1]"));
    }

    @Test
    public void importHash() throws Exception {
	executeFile("/json/import-hmset.txt");
	List<String> keys = commands().keys("beer:*");
	Assertions.assertEquals(4432, keys.size());
	Map<String, String> beer1 = commands().hgetall("beer:1");
	Assertions.assertEquals("Hocus Pocus", beer1.get("name"));
    }
}
