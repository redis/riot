package com.redislabs.riot.file;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.Faker;
import com.redislabs.lettusearch.RediSearchCommands;
import com.redislabs.lettusearch.index.Schema;
import com.redislabs.lettusearch.index.field.GeoField;
import com.redislabs.lettusearch.index.field.NumericField;
import com.redislabs.lettusearch.index.field.PhoneticMatcher;
import com.redislabs.lettusearch.index.field.TextField;
import com.redislabs.lettusearch.search.Document;
import com.redislabs.lettusearch.search.SearchResults;
import com.redislabs.riot.test.BaseTest;
import io.lettuce.core.GeoArgs.Unit;
import io.lettuce.core.ScoredValue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder.DelimitedBuilder;
import org.springframework.batch.item.file.separator.DefaultRecordSeparatorPolicy;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.batch.item.redis.support.KeyValue;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.InputStreamResource;

import java.io.File;
import java.nio.file.Files;
import java.time.Instant;
import java.util.*;
import java.util.zip.GZIPInputStream;

public class TestRiotFile extends BaseTest {

    private final static int COUNT = 2410;

    @Override
    protected int execute(String[] args) throws Exception {
        return new App().execute(args);
    }

    @Override
    protected String applicationName() {
        return "riot-file";
    }

    @Test
    public void exportCsv() throws Exception {
        File file = new File("/tmp/beers.csv");
        file.delete();
        runFile("/import-json-hash.txt");
        runFile("/export-csv.txt");
        String[] header = Files.readAllLines(file.toPath()).get(0).split("\\|");
        FlatFileItemReaderBuilder<Map<String, String>> builder = new FlatFileItemReaderBuilder<>();
        builder.name("flat-file-reader");
        builder.resource(new FileSystemResource(file));
        builder.strict(true);
        builder.saveState(false);
        builder.linesToSkip(1);
        builder.fieldSetMapper(new MapFieldSetMapper());
        builder.recordSeparatorPolicy(new DefaultRecordSeparatorPolicy());
        DelimitedBuilder<Map<String, String>> delimitedBuilder = builder.delimited();
        delimitedBuilder.delimiter("|");
        delimitedBuilder.names(header);
        FlatFileItemReader<Map<String, String>> reader = builder.build();
        List<Map<String, String>> records = readAll(reader);
        Assertions.assertEquals(commands().keys("beer:*").size(), records.size());
    }

    private <T> List<T> readAll(AbstractItemCountingItemStreamItemReader<T> reader) throws Exception {
        reader.open(new ExecutionContext());
        List<T> records = new ArrayList<>();
        T record;
        while ((record = reader.read()) != null) {
            records.add(record);
        }
        reader.close();
        return records;
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void exportJson() throws Exception {
        File file = new File("/tmp/beers.json");
        file.delete();
        runFile("/import-json-hash.txt");
        runFile("/export-json.txt");
        JsonItemReaderBuilder<Map> builder = new JsonItemReaderBuilder<>();
        builder.name("json-file-reader");
        builder.resource(new FileSystemResource(file));
        JacksonJsonObjectReader<Map> objectReader = new JacksonJsonObjectReader<>(Map.class);
        objectReader.setMapper(new ObjectMapper());
        builder.jsonObjectReader(objectReader);
        JsonItemReader<Map> reader = builder.build();
        List<Map> records = readAll(reader);
        Assertions.assertEquals(commands().keys("beer:*").size(), records.size());
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
        runFile("/import-json-hash.txt");
        runFile("/export-json-gz.txt");
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
    public void importCsvHash() throws Exception {
        runFile("/import-csv-hash.txt");
        List<String> keys = commands().keys("beer:*");
        Assertions.assertEquals(COUNT, keys.size());
    }

    @Test
    public void importCsvSearch() throws Exception {
        String FIELD_ABV = "abv";
        String FIELD_NAME = "name";
        String FIELD_STYLE = "style";
        String FIELD_OUNCES = "ounces";
        String INDEX = "beers";
        commands().flushall();
        Schema schema = Schema.builder().field(TextField.builder().name(FIELD_NAME).sortable(true).build()).field(TextField.builder().name(FIELD_STYLE).matcher(PhoneticMatcher.English).sortable(true).build()).field(NumericField.builder().name(FIELD_ABV).sortable(true).build()).field(NumericField.builder().name(FIELD_OUNCES).sortable(true).build()).build();
        commands().create(INDEX, schema, null);
        runFile("/import-csv-search.txt");
        SearchResults<String, String> results = commands().search(INDEX, "*");
        Assertions.assertEquals(COUNT, results.getCount());
    }

    @Test
    public void importCsvProcessorSearchGeo() throws Exception {
        String INDEX = "airports";
        commands().flushall();
        Schema schema = Schema.builder().field(TextField.builder().name("Name").sortable(true).build()).field(GeoField.builder().name("Location").sortable(true).build()).build();
        commands().create(INDEX, schema, null);
        runFile("/import-csv-processor-search-geo.txt");
        SearchResults<String, String> results = commands().search(INDEX, "@Location:[-77 38 50 mi]");
        Assertions.assertEquals(3, results.getCount());
    }

    @Test
    public void importCsvGeo() throws Exception {
        runFile("/import-csv-geo.txt");
        Set<String> results = commands().georadius("airportgeo", -122.4194, 37.7749, 20, Unit.mi);
        Assertions.assertTrue(results.contains("3469"));
        Assertions.assertTrue(results.contains("10360"));
        Assertions.assertTrue(results.contains("8982"));
    }

    @Test
    public void importElasticJson() throws Exception {
        String url = getClass().getClassLoader().getResource("es_test-index.json").getFile();
        runFile("/import-elastic-json.txt", url);
        Assertions.assertEquals(2, commands().keys("estest:*").size());
        Map<String, String> doc1 = commands().hgetall("estest:doc1");
        Assertions.assertEquals("ruan", doc1.get("_source.name"));
        Assertions.assertEquals("3", doc1.get("_source.articles[1]"));
    }

    @Test
    public void importJsonHash() throws Exception {
        runFile("/import-json-hash.txt");
        List<String> keys = commands().keys("beer:*");
        Assertions.assertEquals(4432, keys.size());
        Map<String, String> beer1 = commands().hgetall("beer:1");
        Assertions.assertEquals("Hocus Pocus", beer1.get("name"));
    }

    @Test
    public void importCsvProcessorHashDateFormat() throws Exception {
        runFile("/import-csv-processor-hash-dateformat.txt");
        List<String> keys = commands().keys("event:*");
        Assertions.assertEquals(568, keys.size());
        Map<String, String> event = commands().hgetall("event:248206");
        Instant date = Instant.ofEpochMilli(Long.parseLong(event.get("EpochStart")));
        Assertions.assertTrue(date.isBefore(Instant.now()));
        long index = Long.parseLong(event.get("index"));
        Assertions.assertTrue(index > 0);
    }

    @Test
    public void importCsvProcessorSearch() throws Exception {
        String INDEX = "laevents";
        Schema schema = Schema.builder().field(TextField.builder().name("Title").build()).field(NumericField.builder().name("lon").build()).field(NumericField.builder().name("kat").build()).field(GeoField.builder().name("location").sortable(true).build()).build();
        commands().create(INDEX, schema, null);
        runFile("/import-csv-processor-search.txt");
        SearchResults<String, String> results = commands().search(INDEX, "@location:[-118.446014 33.998415 10 mi]");
        Assertions.assertTrue(results.getCount() > 0);
        for (Document<String, String> result : results) {
            Double lat = Double.parseDouble(result.get("lat"));
            Assertions.assertTrue(lat > 33 && lat < 35);
        }
    }
    
}
