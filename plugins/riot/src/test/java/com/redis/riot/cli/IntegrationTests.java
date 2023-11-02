package com.redis.riot.cli;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.springframework.util.FileCopyUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redis.lettucemod.search.CreateOptions;
import com.redis.lettucemod.search.Document;
import com.redis.lettucemod.search.Field;
import com.redis.lettucemod.search.SearchResults;
import com.redis.lettucemod.search.Suggestion;
import com.redis.lettucemod.search.SuggetOptions;
import com.redis.lettucemod.search.TextField.PhoneticMatcher;
import com.redis.lettucemod.timeseries.MRangeOptions;
import com.redis.lettucemod.timeseries.RangeResult;
import com.redis.lettucemod.timeseries.TimeRange;
import com.redis.riot.core.GeneratorImport;
import com.redis.riot.file.resource.XmlItemReader;
import com.redis.riot.file.resource.XmlItemReaderBuilder;
import com.redis.riot.file.resource.XmlObjectReader;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.gen.GeneratorItemReader;

import io.lettuce.core.GeoArgs;
import io.lettuce.core.Range;
import io.lettuce.core.StreamMessage;
import picocli.CommandLine.ExitCode;
import picocli.CommandLine.ParseResult;

@SuppressWarnings("unchecked")
public abstract class IntegrationTests extends RiotTests {

    public static final int BEER_CSV_COUNT = 2410;

    public static final int BEER_JSON_COUNT = 216;

    private static Path tempDir;

    protected static String name(Map<String, String> beer) {
        return beer.get("name");
    }

    protected static String style(Map<String, String> beer) {
        return beer.get("style");
    }

    protected static double abv(Map<String, String> beer) {
        return Double.parseDouble(beer.get("abv"));
    }

    @BeforeAll
    public void setupFiles() throws IOException {
        tempDir = Files.createTempDirectory(getClass().getName());
    }

    protected List<String> testImport(String filename, String pattern, int count) throws Exception {
        execute(filename);
        List<String> keys = commands.keys(pattern);
        Assertions.assertEquals(count, keys.size());
        return keys;
    }

    private String replace(String file) {
        return file.replace("/tmp", tempDir.toString());
    }

    protected Path tempFile(String filename) throws IOException {
        Path path = tempDir.resolve(filename);
        if (Files.exists(path)) {
            Files.delete(path);
        }
        return path;
    }

    @Test
    void fileImportFW() throws Exception {
        testImport("file-import-fw", "account:*", 5);
        Map<String, String> account101 = commands.hgetall("account:101");
        // Account LastName FirstName Balance CreditLimit AccountCreated Rating
        // 101 Reeves Keanu 9315.45 10000.00 1/17/1998 A
        Assertions.assertEquals("Reeves", account101.get("LastName"));
        Assertions.assertEquals("Keanu", account101.get("FirstName"));
        Assertions.assertEquals("A", account101.get("Rating"));
    }

    @Test
    void fileImportCSV() throws Exception {
        testImport("file-import-csv", "beer:*", BEER_CSV_COUNT);
    }

    @Test
    void fileImportCSVSkipLines() throws Exception {
        testImport("file-import-csv-skiplines", "beer:*", BEER_CSV_COUNT - 10);
    }

    @Test
    void fileImportCSVMax() throws Exception {
        testImport("file-import-csv-max", "beer:*", 12);
    }

    @Test
    void fileImportPSV() throws Exception {
        testImport("file-import-psv", "sample:*", 3);
    }

    @Test
    void fileImportTSV() throws Exception {
        testImport("file-import-tsv", "sample:*", 4);
    }

    @Test
    void fileImportType() throws Exception {
        testImport("file-import-type", "sample:*", 3);
    }

    @Test
    void fileImportExclude() throws Exception {
        execute("file-import-exclude");
        Map<String, String> beer1036 = commands.hgetall("beer:1036");
        Assertions.assertEquals("Lower De Boom", name(beer1036));
        Assertions.assertEquals("American Barleywine", style(beer1036));
        Assertions.assertEquals("368", beer1036.get("brewery_id"));
        Assertions.assertFalse(beer1036.containsKey("row"));
        Assertions.assertFalse(beer1036.containsKey("ibu"));
    }

    @Test
    void fileImportInclude() throws Exception {
        execute("file-import-include");
        Map<String, String> beer1036 = commands.hgetall("beer:1036");
        Assertions.assertEquals(3, beer1036.size());
        Assertions.assertEquals("Lower De Boom", name(beer1036));
        Assertions.assertEquals("American Barleywine", style(beer1036));
        Assertions.assertEquals(0.099, abv(beer1036));
    }

    @Test
    void fileImportFilter() throws Exception {
        testImport("file-import-filter", "beer:*", 424);
    }

    @Test
    void fileImportRegex() throws Exception {
        execute("file-import-regex");
        Map<String, String> airport1 = commands.hgetall("airport:1");
        Assertions.assertEquals("Pacific", airport1.get("region"));
        Assertions.assertEquals("Port_Moresby", airport1.get("city"));
    }

    @Test
    void fileImportGlob() throws Exception {
        execute("file-import-glob", this::executeImportGlob);
        List<String> keys = commands.keys("beer:*");
        Assertions.assertEquals(BEER_CSV_COUNT, keys.size());
    }

    private int executeImportGlob(ParseResult parseResult) {

        FileImportCommand command = command(parseResult);
        try {
            Path dir = Files.createTempDirectory("import-glob");
            FileCopyUtils.copy(getClass().getClassLoader().getResourceAsStream("files/beers1.csv"),
                    Files.newOutputStream(dir.resolve("beers1.csv")));
            FileCopyUtils.copy(getClass().getClassLoader().getResourceAsStream("files/beers2.csv"),
                    Files.newOutputStream(dir.resolve("beers2.csv")));
            File file = new File(command.args.files.get(0));
            command.args.files = Arrays.asList(dir.resolve(file.getName()).toString());
        } catch (IOException e) {
            throw new RuntimeException("Could not configure import-glob", e);
        }
        return ExitCode.OK;
    }

    @Test
    void fileImportGeoadd() throws Exception {
        execute("file-import-geoadd");
        Set<String> results = commands.georadius("airportgeo", -21, 64, 200, GeoArgs.Unit.mi);
        Assertions.assertTrue(results.contains("18"));
        Assertions.assertTrue(results.contains("19"));
        Assertions.assertTrue(results.contains("11"));
    }

    @Test
    void fileImportGeoProcessor() throws Exception {
        execute("file-import-geo-processor");
        Map<String, String> airport3469 = commands.hgetall("airport:18");
        Assertions.assertEquals("-21.9405994415,64.1299972534", airport3469.get("location"));
    }

    @Test
    void fileImportProcess() throws Exception {
        testImport("file-import-process", "event:*", 568);
        Map<String, String> event = commands.hgetall("event:248206");
        Instant date = Instant.ofEpochMilli(Long.parseLong(event.get("EpochStart")));
        Assertions.assertTrue(date.isBefore(Instant.now()));
    }

    @Test
    void fileImportProcessElvis() throws Exception {
        testImport("file-import-process-elvis", "beer:*", BEER_CSV_COUNT);
        Map<String, String> beer1436 = commands.hgetall("beer:1436");
        Assertions.assertEquals("10", beer1436.get("ibu"));
    }

    @Test
    void fileImportMultiCommands() throws Exception {
        execute("file-import-multi-commands");
        List<String> beers = commands.keys("beer:*");
        Assertions.assertEquals(BEER_CSV_COUNT, beers.size());
        for (String beer : beers) {
            Map<String, String> hash = commands.hgetall(beer);
            Assertions.assertTrue(hash.containsKey("name"));
            Assertions.assertTrue(hash.containsKey("brewery_id"));
        }
        Set<String> breweries = commands.smembers("breweries");
        Assertions.assertEquals(558, breweries.size());
    }

    @Test
    void fileImportBad() throws Exception {
        Assertions.assertEquals(0, execute("file-import-bad"));
    }

    @Test
    void fileImportGCS() throws Exception {
        testImport("file-import-gcs", "beer:*", 4432);
        Map<String, String> beer1 = commands.hgetall("beer:1");
        Assertions.assertEquals("Hocus Pocus", name(beer1));
    }

    @Test
    void fileImportS3() throws Exception {
        testImport("file-import-s3", "beer:*", 4432);
        Map<String, String> beer1 = commands.hgetall("beer:1");
        Assertions.assertEquals("Hocus Pocus", name(beer1));
    }

    @SuppressWarnings("rawtypes")
    @Test
    void fileDumpImport(TestInfo info) throws Exception {
        List<KeyValue> records = exportToJsonFile(info);
        commands.flushall();
        execute("dump-import", this::executeFileDumpImport);
        awaitUntil(() -> records.size() == Math.toIntExact(commands.dbsize()));
    }

    private int executeFileDumpImport(ParseResult parseResult) {
        FileDumpImportCommand command = command(parseResult);
        command.args.files = command.args.files.stream().map(this::replace).collect(Collectors.toList());
        return ExitCode.OK;
    }

    private int executeFileDumpExport(ParseResult parseResult) {
        FileDumpExportCommand command = command(parseResult);
        command.args.file = replace(command.args.file);
        return ExitCode.OK;
    }

    @Test
    @Disabled("Needs update")
    void fileImportJSONElastic() throws Exception {
        execute("file-import-json-elastic");
        Assertions.assertEquals(2, commands.keys("estest:*").size());
        Map<String, String> doc1 = commands.hgetall("estest:doc1");
        Assertions.assertEquals("ruan", doc1.get("_source.name"));
        Assertions.assertEquals("3", doc1.get("_source.articles[1]"));
    }

    @Test
    void fileImportJSON() throws Exception {
        testImport("file-import-json", "beer:*", BEER_JSON_COUNT);
        Map<String, String> beer1 = commands.hgetall("beer:1");
        Assertions.assertEquals("Hocus Pocus", beer1.get("name"));
    }

    @Test
    void fileImportXML() throws Exception {
        testImport("file-import-xml", "trade:*", 3);
        Map<String, String> trade1 = commands.hgetall("trade:1");
        Assertions.assertEquals("XYZ0001", trade1.get("isin"));
    }

    @SuppressWarnings("rawtypes")
    @Test
    void fileExportJSON(TestInfo info) throws Exception {
        List<KeyValue> records = exportToJsonFile(info);
        Assertions.assertEquals(commands.dbsize(), records.size());
    }

    @SuppressWarnings("rawtypes")
    @Test
    @Disabled("Needs update")
    void fileExportJSONGz() throws Exception {
        Path file = tempFile("beers.json.gz");
        execute("file-import-json");
        execute("file-export-json-gz", this::executeFileDumpExport);
        JsonItemReaderBuilder<Map> builder = new JsonItemReaderBuilder<>();
        builder.name("json-file-reader");
        FileSystemResource resource = new FileSystemResource(file);
        builder.resource(new InputStreamResource(new GZIPInputStream(resource.getInputStream()), resource.getDescription()));
        JacksonJsonObjectReader<Map> objectReader = new JacksonJsonObjectReader<>(Map.class);
        objectReader.setMapper(new ObjectMapper());
        builder.jsonObjectReader(objectReader);
        JsonItemReader<Map> reader = builder.build();
        reader.open(new ExecutionContext());
        try {
            List<Map> records = readAll(reader);
            Assertions.assertEquals(commands.keys("beer:*").size(), records.size());
        } finally {
            reader.close();
        }
    }

    @SuppressWarnings("rawtypes")
    private List<KeyValue> exportToJsonFile(TestInfo info) throws Exception {
        String filename = "file-export-json";
        Path file = tempFile("redis.json");
        generate(info);
        Thread.sleep(300);
        execute(filename, this::executeFileDumpExport);
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

    @SuppressWarnings("rawtypes")
    @Test
    void fileExportXml(TestInfo info) throws Exception {
        String filename = "file-export-xml";
        generate(info);
        Path file = tempFile("redis.xml");
        execute(filename, this::executeFileDumpExport);
        XmlItemReaderBuilder<KeyValue> builder = new XmlItemReaderBuilder<>();
        builder.name("xml-file-reader");
        builder.resource(new FileSystemResource(file));
        XmlObjectReader<KeyValue> xmlObjectReader = new XmlObjectReader<>(KeyValue.class);
        xmlObjectReader.setMapper(new XmlMapper());
        builder.xmlObjectReader(xmlObjectReader);
        XmlItemReader<KeyValue> reader = builder.build();
        reader.open(new ExecutionContext());
        List<KeyValue> records = readAll(reader);
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

    @Test
    void fileImportJSONGzip() throws Exception {
        testImport("file-import-json-gz", "beer:*", BEER_JSON_COUNT);
    }

    @Test
    void fileImportSugadd() throws Exception {
        assertExecutionSuccessful(execute("file-import-sugadd"));
        List<Suggestion<String>> suggestions = commands.ftSugget("names", "Bea",
                SuggetOptions.builder().withPayloads(true).build());
        Assertions.assertEquals(5, suggestions.size());
        Assertions.assertEquals("American Blonde Ale", suggestions.get(0).getPayload());
    }

    @Test
    void fileImportElasticJSON() throws Exception {
        assertExecutionSuccessful(execute("file-import-json-elastic-jsonset"));
        Assertions.assertEquals(2, commands.keys("elastic:*").size());
        ObjectMapper mapper = new ObjectMapper();
        String doc1 = commands.jsonGet("elastic:doc1");
        String expected = "{\"_index\":\"test-index\",\"_type\":\"docs\",\"_id\":\"doc1\",\"_score\":1,\"_source\":{\"name\":\"ruan\",\"age\":30,\"articles\":[\"1\",\"3\"]}}";
        Assertions.assertEquals(mapper.readTree(expected), mapper.readTree(doc1));
    }

    @Test
    void fakerHash() throws Exception {
        List<String> keys = testImport("faker-import-hset", "person:*", 1000);
        Map<String, String> person = commands.hgetall(keys.get(0));
        Assertions.assertTrue(person.containsKey("firstName"));
        Assertions.assertTrue(person.containsKey("lastName"));
        Assertions.assertTrue(person.containsKey("address"));
    }

    @Test
    void fakerSet() throws Exception {
        execute("faker-import-sadd");
        Set<String> names = commands.smembers("got:characters");
        Assertions.assertTrue(names.size() > 10);
        for (String name : names) {
            Assertions.assertFalse(name.isEmpty());
        }
    }

    @Test
    void fakerZset() throws Exception {
        execute("faker-import-zadd");
        List<String> keys = commands.keys("leases:*");
        Assertions.assertTrue(keys.size() > 100);
        String key = keys.get(0);
        Assertions.assertTrue(commands.zcard(key) > 0);
    }

    @Test
    void fakerStream() throws Exception {
        execute("faker-import-xadd");
        List<StreamMessage<String, String>> messages = commands.xrange("teststream:1", Range.unbounded());
        Assertions.assertTrue(messages.size() > 0);
    }

    @Test
    @Disabled("Needs update")
    void fakerInfer() throws Exception {
        String INDEX = "beerIdx";
        String FIELD_ID = "id";
        String FIELD_ABV = "abv";
        String FIELD_NAME = "name";
        String FIELD_STYLE = "style";
        String FIELD_OUNCES = "ounces";
        commands.ftCreate(INDEX, CreateOptions.<String, String> builder().prefix("beer:").build(),
                Field.tag(FIELD_ID).sortable().build(), Field.text(FIELD_NAME).sortable().build(),
                Field.text(FIELD_STYLE).matcher(PhoneticMatcher.ENGLISH).sortable().build(),
                Field.numeric(FIELD_ABV).sortable().build(), Field.numeric(FIELD_OUNCES).sortable().build());
        execute("faker-import-infer");
        SearchResults<String, String> results = commands.ftSearch(INDEX, "*");
        Assertions.assertEquals(1000, results.getCount());
        Document<String, String> doc1 = results.get(0);
        Assertions.assertNotNull(doc1.get(FIELD_ABV));
    }

    @Test
    @Disabled("Flaky test")
    void fakerTsAdd() throws Exception {
        execute("faker-import-tsadd");
        Assertions.assertEquals(10, commands.tsRange("ts:gen", TimeRange.unbounded()).size());
    }

    @Test
    void fakerTsAddWithOptions() throws Exception {
        execute("faker-import-tsadd-options");
        List<RangeResult<String, String>> results = commands.tsMrange(TimeRange.unbounded(),
                MRangeOptions.<String, String> filters("character1=Einstein").build());
        Assertions.assertFalse(results.isEmpty());
    }

    @Test
    void generateTypes() throws Exception {
        execute("generate");
        Assertions.assertEquals(Math.min(GeneratorImport.DEFAULT_COUNT, GeneratorItemReader.DEFAULT_KEY_RANGE.getMax()),
                commands.dbsize());
    }

}
