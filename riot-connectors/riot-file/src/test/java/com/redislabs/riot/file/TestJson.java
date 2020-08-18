package com.redislabs.riot.file;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.redislabs.lettusearch.search.SearchResults;
import com.redislabs.riot.WritableDocument;
import com.redislabs.riot.test.DataPopulator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.batch.item.redis.support.KeyValue;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.InputStreamResource;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

public class TestJson extends AbstractFileTest {

    @Test
    public void exportSearch() throws Exception {
        Path file = tempFile("beers.json");
        TestCsv.createBeerIndex(connection());
        executeFile("/csv/import-search.txt");
        executeFile("/json/export-search.txt");
        JsonItemReaderBuilder<WritableDocument> builder = new JsonItemReaderBuilder<>();
        builder.name("search-json-file-reader");
        builder.resource(new FileSystemResource(file));
        JacksonJsonObjectReader<WritableDocument> objectReader = new JacksonJsonObjectReader<>(WritableDocument.class);
        objectReader.setMapper(new ObjectMapper());
        builder.jsonObjectReader(objectReader);
        JsonItemReader<WritableDocument> reader = builder.build();
        List<WritableDocument> records = readAll(reader);
        SearchResults<String, String> results = commands().search("beers", "*");
        Assertions.assertEquals(results.getCount(), records.size());
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void exportKeyValues() throws Exception {
        Path file = tempFile("riot-keyvalues.json");
        DataPopulator.builder().connection(connection()).build().run();
        executeFile("/json/export-keyvalues.txt");
        JsonItemReaderBuilder<KeyValue> builder = new JsonItemReaderBuilder<>();
        builder.name("json-keyvalues-file-reader");
        builder.resource(new FileSystemResource(file));
        JacksonJsonObjectReader<KeyValue> objectReader = new JacksonJsonObjectReader<>(KeyValue.class);
        objectReader.setMapper(new ObjectMapper());
        builder.jsonObjectReader(objectReader);
        JsonItemReader<KeyValue> reader = builder.build();
        List<KeyValue> records = readAll(reader);
        Assertions.assertEquals(commands().dbsize(), records.size());
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void exportGzip() throws Exception {
        Path file = tempFile("beers.json.gz");
        executeFile("/json/import-hash.txt");
        executeFile("/json/export-gzip.txt");
        JsonItemReaderBuilder<Map> builder = new JsonItemReaderBuilder<>();
        builder.name("json-file-reader");
        FileSystemResource resource = new FileSystemResource(file);
        builder.resource(new InputStreamResource(new GZIPInputStream(resource.getInputStream()), resource.getDescription()));
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
        executeFile("/json/import-hash.txt");
        List<String> keys = commands().keys("beer:*");
        Assertions.assertEquals(4432, keys.size());
        Map<String, String> beer1 = commands().hgetall("beer:1");
        Assertions.assertEquals("Hocus Pocus", beer1.get("name"));
    }
}
