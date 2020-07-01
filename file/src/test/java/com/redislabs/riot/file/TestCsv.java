package com.redislabs.riot.file;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.index.Schema;
import com.redislabs.lettusearch.index.field.GeoField;
import com.redislabs.lettusearch.index.field.NumericField;
import com.redislabs.lettusearch.index.field.PhoneticMatcher;
import com.redislabs.lettusearch.index.field.TextField;
import com.redislabs.lettusearch.search.Document;
import com.redislabs.lettusearch.search.SearchResults;
import io.lettuce.core.GeoArgs;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.separator.DefaultRecordSeparatorPolicy;
import org.springframework.core.io.FileSystemResource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestCsv extends AbstractFileTest {

    private final static String FIELD_ABV = "abv";
    private final static String FIELD_NAME = "name";
    private final static String FIELD_STYLE = "style";
    private final static String FIELD_OUNCES = "ounces";
    public final static String INDEX = "beers";

    public static void createBeerIndex(StatefulRediSearchConnection<String, String> connection) {
        connection.sync().flushall();
        Schema schema = Schema.builder().field(TextField.builder().name(FIELD_NAME).sortable(true).build()).field(TextField.builder().name(FIELD_STYLE).matcher(PhoneticMatcher.English).sortable(true).build()).field(NumericField.builder().name(FIELD_ABV).sortable(true).build()).field(NumericField.builder().name(FIELD_OUNCES).sortable(true).build()).build();
        connection.sync().create(INDEX, schema, null);
    }

    @Test
    public void export() throws Exception {
        Path file = tempFile("beers.csv");
        runFile("/json/import-hash.txt");
        runFile("/csv/export-hash.txt");
        String[] header = Files.readAllLines(file).get(0).split("\\|");
        FlatFileItemReaderBuilder<Map<String, String>> builder = new FlatFileItemReaderBuilder<>();
        builder.name("flat-file-reader");
        builder.resource(new FileSystemResource(file));
        builder.strict(true);
        builder.saveState(false);
        builder.linesToSkip(1);
        builder.fieldSetMapper(new MapFieldSetMapper());
        builder.recordSeparatorPolicy(new DefaultRecordSeparatorPolicy());
        FlatFileItemReaderBuilder.DelimitedBuilder<Map<String, String>> delimitedBuilder = builder.delimited();
        delimitedBuilder.delimiter("|");
        delimitedBuilder.names(header);
        FlatFileItemReader<Map<String, String>> reader = builder.build();
        List<Map<String, String>> records = readAll(reader);
        Assertions.assertEquals(commands().keys("beer:*").size(), records.size());
    }

    @Test
    public void importHash() throws Exception {
        runFile("/csv/import-hash.txt");
        List<String> keys = commands().keys("beer:*");
        Assertions.assertEquals(COUNT, keys.size());
    }

    @Test
    public void importMultiHash() throws Exception {
        runFile("/csv/import-multi-hash.txt");
        List<String> keys = commands().keys("beer:*");
        Assertions.assertEquals(COUNT, keys.size());
    }

    @Test
    public void importSearch() throws Exception {
        createBeerIndex(connection());
        runFile("/csv/import-search.txt");
        SearchResults<String, String> results = commands().search(INDEX, "*");
        Assertions.assertEquals(COUNT, results.getCount());
    }

    @Test
    public void importProcessorSearchGeo() throws Exception {
        String INDEX = "airports";
        commands().flushall();
        Schema schema = Schema.builder().field(TextField.builder().name("Name").sortable(true).build()).field(GeoField.builder().name("Location").sortable(true).build()).build();
        commands().create(INDEX, schema, null);
        runFile("/csv/import-search-geo-processor.txt");
        SearchResults<String, String> results = commands().search(INDEX, "@Location:[-77 38 50 mi]");
        Assertions.assertEquals(3, results.getCount());
    }

    @Test
    public void importGeo() throws Exception {
        runFile("/csv/import-geo.txt");
        Set<String> results = commands().georadius("airportgeo", -122.4194, 37.7749, 20, GeoArgs.Unit.mi);
        Assertions.assertTrue(results.contains("3469"));
        Assertions.assertTrue(results.contains("10360"));
        Assertions.assertTrue(results.contains("8982"));
    }


    @Test
    public void importProcessorHashDateFormat() throws Exception {
        runFile("/csv/import-hash-processor.txt");
        List<String> keys = commands().keys("event:*");
        Assertions.assertEquals(568, keys.size());
        Map<String, String> event = commands().hgetall("event:248206");
        Instant date = Instant.ofEpochMilli(Long.parseLong(event.get("EpochStart")));
        Assertions.assertTrue(date.isBefore(Instant.now()));
        long index = Long.parseLong(event.get("index"));
        Assertions.assertTrue(index > 0);
    }

    @Test
    public void importProcessorSearch() throws Exception {
        String INDEX = "laevents";
        Schema schema = Schema.builder().field(TextField.builder().name("Title").build()).field(NumericField.builder().name("lon").build()).field(NumericField.builder().name("kat").build()).field(GeoField.builder().name("location").sortable(true).build()).build();
        commands().create(INDEX, schema, null);
        runFile("/csv/import-search-processor.txt");
        SearchResults<String, String> results = commands().search(INDEX, "@location:[-118.446014 33.998415 10 mi]");
        Assertions.assertTrue(results.getCount() > 0);
        for (Document<String, String> result : results) {
            double lat = Double.parseDouble(result.get("lat"));
            Assertions.assertTrue(lat > 33 && lat < 35);
        }
    }
}
