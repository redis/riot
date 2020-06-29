package com.redislabs.riot.file;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.Faker;
import com.redislabs.lettusearch.RediSearchCommands;
import com.redislabs.lettusearch.search.SearchResults;
import com.redislabs.riot.WritableDocument;
import com.redislabs.riot.test.DataPopulator;
import io.lettuce.core.ScoredValue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.batch.item.redis.support.KeyValue;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.InputStreamResource;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.zip.GZIPInputStream;

public class TestJson extends AbstractFileTest {

    @SuppressWarnings("rawtypes")
    @Test
    public void exportJsonKeyValues() throws Exception {
        File file = new File("/tmp/riot-keyvalues.json");
        file.delete();
        DataPopulator.builder().connection(connection).build().run();
        runFile("/json/export-keyvalues.txt");
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

    @Test
    public void exportSearchJson() throws Exception {
        File file = new File("/tmp/beers.json");
        file.delete();
        TestCsv.createBeerIndex(connection);
        runFile("/csv/import-search.txt");
        runFile("/json/export-search.txt");
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

    @Test
    public void exportDataStructuresToJson() throws Exception {
        File file = new File("/tmp/datastructures.json");
        file.delete();
        Faker faker = new Faker();
        Random random = new Random();
        RediSearchCommands<String, String> commands = commands();
        int count = 100;
        int valueSize = 10;
        for (int index = 0; index < count; index++) {
            Map<String, String> hash = new HashMap<>();
            String[] values = new String[valueSize];
            ScoredValue<String>[] scoredValues = new ScoredValue[valueSize];
            for (int valueIndex = 0; valueIndex < valueSize; valueIndex++) {
                hash.put("field" + valueIndex, faker.yoda().quote());
                values[valueIndex] = faker.beer().name();
                scoredValues[valueIndex] = ScoredValue.fromNullable(valueIndex, faker.rockBand().name());
            }
            commands.setex("string:" + index, faker.number().numberBetween(0, 3600), faker.backToTheFuture().quote());
            commands.hmset("hash:" + index, hash);
            commands.rpush("list:" + index, values);
            commands.sadd("set:" + index, values);
            commands.zadd("zset:" + index, scoredValues);
            commands.xadd("stream:" + index, hash);
            commands.geoadd("geo:" + index, faker.number().randomDouble(5, -90, 90), faker.number().randomDouble(5, -80, 80), faker.rickAndMorty().character());
        }
        runCommand("export /tmp/datastructures.json");
        JsonItemReaderBuilder<KeyValue> builder = new JsonItemReaderBuilder<>();
        builder.name("datastructures-json-file-reader");
        builder.resource(new FileSystemResource(file));
        JacksonJsonObjectReader<KeyValue> objectReader = new JacksonJsonObjectReader<>(KeyValue.class);
        objectReader.setMapper(new ObjectMapper());
        builder.jsonObjectReader(objectReader);
        JsonItemReader<KeyValue> reader = builder.build();
        List<KeyValue> records = readAll(reader);
        Assertions.assertEquals(commands.dbsize(), records.size());
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void exportJsonGz() throws Exception {
        File file = new File("/tmp/beers.json.gz");
        file.delete();
        runFile("/json/import-hash.txt");
        runFile("/json/export-gzip.txt");
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
    public void importElasticJson() {
        String url = getClass().getClassLoader().getResource("json/es_test-index.json").getFile();
        runFile("/json/import-elastic.txt", url);
        Assertions.assertEquals(2, commands().keys("estest:*").size());
        Map<String, String> doc1 = commands().hgetall("estest:doc1");
        Assertions.assertEquals("ruan", doc1.get("_source.name"));
        Assertions.assertEquals("3", doc1.get("_source.articles[1]"));
    }

    @Test
    public void importJsonHash() {
        runFile("/json/import-hash.txt");
        List<String> keys = commands().keys("beer:*");
        Assertions.assertEquals(4432, keys.size());
        Map<String, String> beer1 = commands().hgetall("beer:1");
        Assertions.assertEquals("Hocus Pocus", beer1.get("name"));
    }
}
