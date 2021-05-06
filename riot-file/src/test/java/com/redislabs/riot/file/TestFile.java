package com.redislabs.riot.file;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redislabs.riot.AbstractRiotIntegrationTest;
import com.redislabs.riot.redis.HsetCommand;
import com.redislabs.testcontainers.RedisContainer;
import io.lettuce.core.GeoArgs;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.sync.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.batch.item.redis.support.DataStructure;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.batch.item.xml.XmlItemReader;
import org.springframework.batch.item.xml.XmlObjectReader;
import org.springframework.batch.item.xml.support.XmlItemReaderBuilder;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.InputStreamResource;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

@SuppressWarnings("unchecked")
public class TestFile extends AbstractRiotIntegrationTest {

    protected final static int COUNT = 2410;

    private static Path tempDir;

    @BeforeAll
    public static void setupAll() throws IOException {
        tempDir = Files.createTempDirectory(TestFile.class.getName());
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


    protected static String name(Map<String, String> beer) {
        return beer.get("name");
    }

    protected static String style(Map<String, String> beer) {
        return beer.get("style");
    }

    protected static double abv(Map<String, String> beer) {
        return Double.parseDouble(beer.get("abv"));
    }

    protected <T> List<T> readAll(AbstractItemCountingItemStreamItemReader<T> reader) throws Exception {
        reader.open(new ExecutionContext());
        List<T> records = new ArrayList<>();
        T record;
        while ((record = reader.read()) != null) {
            records.add(record);
        }
        reader.close();
        return records;
    }

    @Override
    protected RiotFile app() {
        return new RiotFile();
    }

    @ParameterizedTest
    @MethodSource("containers")
    public void importFW(RedisContainer container) throws Exception {
        execute("import-fw", container);
        RedisKeyCommands<String, String> sync = sync(container);
        List<String> keys = sync.keys("account:*");
        Assertions.assertEquals(5, keys.size());
        RedisHashCommands<String, String> hash = sync(container);
        Map<String, String> account101 = hash.hgetall("account:101");
        // Account LastName        FirstName       Balance     CreditLimit   AccountCreated  Rating
        // 101     Reeves          Keanu           9315.45     10000.00      1/17/1998       A
        Assertions.assertEquals("Reeves", account101.get("LastName"));
        Assertions.assertEquals("Keanu", account101.get("FirstName"));
        Assertions.assertEquals("A", account101.get("Rating"));
    }

    @ParameterizedTest
    @MethodSource("containers")
    public void importCSV(RedisContainer container) throws Exception {
        execute("import-csv", container);
        RedisKeyCommands<String, String> sync = sync(container);
        List<String> keys = sync.keys("beer:*");
        Assertions.assertEquals(COUNT, keys.size());
    }

    @ParameterizedTest
    @MethodSource("containers")
    public void importPSV(RedisContainer container) throws Exception {
        execute("import-psv", container);
        RedisKeyCommands<String, String> sync = sync(container);
        List<String> keys = sync.keys("sample:*");
        Assertions.assertEquals(3, keys.size());
    }

    @ParameterizedTest
    @MethodSource("containers")
    public void importTSV(RedisContainer container) throws Exception {
        execute("import-tsv", container);
        RedisKeyCommands<String, String> sync = sync(container);
        List<String> keys = sync.keys("sample:*");
        Assertions.assertEquals(4, keys.size());
    }

    @ParameterizedTest
    @MethodSource("containers")
    public void importType(RedisContainer container) throws Exception {
        execute("import-type", container);
        RedisKeyCommands<String, String> sync = sync(container);
        List<String> keys = sync.keys("sample:*");
        Assertions.assertEquals(3, keys.size());
    }

    @ParameterizedTest
    @MethodSource("containers")
    public void importExclude(RedisContainer container) throws Exception {
        execute("import-exclude", container);
        RedisHashCommands<String, String> sync = sync(container);
        Map<String, String> beer1036 = sync.hgetall("beer:1036");
        Assertions.assertEquals("Lower De Boom", name(beer1036));
        Assertions.assertEquals("American Barleywine", style(beer1036));
        Assertions.assertEquals("368", beer1036.get("brewery_id"));
        Assertions.assertFalse(beer1036.containsKey("row"));
        Assertions.assertFalse(beer1036.containsKey("ibu"));
    }

    //    @ParameterizedTest
    //    @MethodSource("containers")
    //    public void importExcludeAPI(RedisContainer container) throws Exception {
    //        // riot-file import http://developer.redislabs.com/riot/beers.csv --header hset --keyspace beer --keys id --exclude row ibu
    //        FileImportCommand.builder().file("http://developer.redislabs.com/riot/beers.csv").options(FileImportOptions.builder().header(true).build()).
    //        sync(container);
    //        Map<String, String> beer1036 = sync.hgetall("beer:1036");
    //        Assertions.assertEquals("Lower De Boom", name(beer1036));
    //        Assertions.assertEquals("American Barleywine", style(beer1036));
    //        Assertions.assertEquals("368", beer1036.get("brewery_id"));
    //        Assertions.assertFalse(beer1036.containsKey("row"));
    //        Assertions.assertFalse(beer1036.containsKey("ibu"));
    //    }

    @ParameterizedTest
    @MethodSource("containers")
    public void importInclude(RedisContainer container) throws Exception {
        execute("import-include", container);
        RedisHashCommands<String, String> sync = sync(container);
        Map<String, String> beer1036 = sync.hgetall("beer:1036");
        Assertions.assertEquals(3, beer1036.size());
        Assertions.assertEquals("Lower De Boom", name(beer1036));
        Assertions.assertEquals("American Barleywine", style(beer1036));
        Assertions.assertEquals(0.099, abv(beer1036));
    }

    @ParameterizedTest
    @MethodSource("containers")
    public void importFilter(RedisContainer container) throws Exception {
        execute("import-filter", container);
        RedisKeyCommands<String, String> sync = sync(container);
        List<String> keys = sync.keys("beer:*");
        Assertions.assertEquals(424, keys.size());
    }

    @ParameterizedTest
    @MethodSource("containers")
    public void importRegex(RedisContainer container) throws Exception {
        execute("import-regex", container);
        RedisHashCommands<String, String> sync = sync(container);
        Map<String, String> airport1 = sync.hgetall("airport:1");
        Assertions.assertEquals("Pacific", airport1.get("region"));
        Assertions.assertEquals("Port_Moresby", airport1.get("city"));
    }

    @ParameterizedTest
    @MethodSource("containers")
    public void importGlob(RedisContainer container) throws Exception {
        execute("import-glob", container);
        RedisKeyCommands<String, String> sync = sync(container);
        List<String> keys = sync.keys("beer:*");
        Assertions.assertEquals(COUNT, keys.size());
    }

    @ParameterizedTest
    @MethodSource("containers")
    public void importGeoadd(RedisContainer container) throws Exception {
        execute("import-geoadd", container);
        RedisGeoCommands<String, String> sync = sync(container);
        Set<String> results = sync.georadius("airportgeo", -122.4194, 37.7749, 20, GeoArgs.Unit.mi);
        Assertions.assertTrue(results.contains("3469"));
        Assertions.assertTrue(results.contains("10360"));
        Assertions.assertTrue(results.contains("8982"));
    }

    @ParameterizedTest
    @MethodSource("containers")
    public void importGeoProcessor(RedisContainer container) throws Exception {
        execute("import-geo-processor", container);
        RedisHashCommands<String, String> sync = sync(container);
        Map<String, String> airport3469 = sync.hgetall("airport:3469");
        Assertions.assertEquals("-122.375,37.61899948120117", airport3469.get("location"));
    }

    @ParameterizedTest
    @MethodSource("containers")
    public void importProcess(RedisContainer container) throws Exception {
        execute("import-process", container);
        RedisKeyCommands<String, String> sync = sync(container);
        List<String> keys = sync.keys("event:*");
        Assertions.assertEquals(568, keys.size());
        RedisHashCommands<String, String> hash = sync(container);
        Map<String, String> event = hash.hgetall("event:248206");
        Instant date = Instant.ofEpochMilli(Long.parseLong(event.get("EpochStart")));
        Assertions.assertTrue(date.isBefore(Instant.now()));
        long index = Long.parseLong(event.get("index"));
        Assertions.assertTrue(index > 0);
    }

    @ParameterizedTest
    @MethodSource("containers")
    public void importProcessElvis(RedisContainer container) throws Exception {
        execute("import-process-elvis", container);
        RedisKeyCommands<String, String> sync = sync(container);
        List<String> keys = sync.keys("beer:*");
        Assertions.assertEquals(COUNT, keys.size());
        Map<String, String> beer1436 = ((RedisHashCommands<String, String>) sync).hgetall("beer:1436");
        Assertions.assertEquals("10", beer1436.get("ibu"));
    }

    @ParameterizedTest
    @MethodSource("containers")
    public void importMultiCommands(RedisContainer container) throws Exception {
        execute("import-multi-commands", container);
        RedisKeyCommands<String, String> sync = sync(container);
        List<String> beers = sync.keys("beer:*");
        Assertions.assertEquals(2410, beers.size());
        for (String beer : beers) {
            Map<String, String> hash = ((RedisHashCommands<String, String>) sync).hgetall(beer);
            Assertions.assertTrue(hash.containsKey("name"));
            Assertions.assertTrue(hash.containsKey("brewery_id"));
        }
        RedisSetCommands<String, String> set = sync(container);
        Set<String> breweries = set.smembers("breweries");
        Assertions.assertEquals(558, breweries.size());
    }

    @ParameterizedTest
    @MethodSource("containers")
    public void importBad(RedisContainer container) throws Exception {
        execute("import-bad", container);
    }

    @ParameterizedTest
    @MethodSource("containers")
    public void importGCS(RedisContainer container) throws Exception {
        execute("import-gcs", container);
        RedisKeyCommands<String, String> sync = sync(container);
        List<String> keys = sync.keys("beer:*");
        Assertions.assertEquals(4432, keys.size());
        Map<String, String> beer1 = ((RedisHashCommands<String, String>) sync).hgetall("beer:1");
        Assertions.assertEquals("Hocus Pocus", name(beer1));
    }

    @ParameterizedTest
    @MethodSource("containers")
    public void importS3(RedisContainer container) throws Exception {
        execute("import-s3", container);
        RedisKeyCommands<String, String> sync = sync(container);
        List<String> keys = sync.keys("beer:*");
        Assertions.assertEquals(4432, keys.size());
        Map<String, String> beer1 = ((RedisHashCommands<String, String>) sync).hgetall("beer:1");
        Assertions.assertEquals("Hocus Pocus", name(beer1));
    }

    @ParameterizedTest
    @MethodSource("containers")
    public void importDump(RedisContainer container) throws Exception {
        List<DataStructure> records = exportToList(container);
        RedisServerCommands<String, String> sync = sync(container);
        sync.flushall();
        execute("import-dump", container, this::configureDumpFileImportCommand);
        Assertions.assertEquals(records.size(), sync.dbsize());
    }

    private void configureDumpFileImportCommand(CommandLine.ParseResult parseResult) {
        DumpFileImportCommand command = parseResult.subcommand().commandSpec().commandLine().getCommand();
        command.setFiles(command.getFiles().stream().map(this::replace).collect(Collectors.toList()));
    }

    private void configureExportCommand(CommandLine.ParseResult parseResult) {
        FileExportCommand command = parseResult.subcommand().commandSpec().commandLine().getCommand();
        command.setFile(replace(command.getFile()));
    }

    @ParameterizedTest
    @MethodSource("containers")
    public void importJsonElastic(RedisContainer container) throws Exception {
        execute("import-json-elastic", container);
        RedisKeyCommands<String, String> sync = sync(container);
        Assertions.assertEquals(2, sync.keys("estest:*").size());
        Map<String, String> doc1 = ((RedisHashCommands<String, String>) sync).hgetall("estest:doc1");
        Assertions.assertEquals("ruan", doc1.get("_source.name"));
        Assertions.assertEquals("3", doc1.get("_source.articles[1]"));
    }

    @ParameterizedTest
    @MethodSource("containers")
    public void importJson(RedisContainer container) throws Exception {
        execute("import-json", container);
        RedisKeyCommands<String, String> sync = sync(container);
        List<String> keys = sync.keys("beer:*");
        Assertions.assertEquals(4432, keys.size());
        Map<String, String> beer1 = ((RedisHashCommands<String, String>) sync).hgetall("beer:1");
        Assertions.assertEquals("Hocus Pocus", beer1.get("name"));
    }

    @ParameterizedTest
    @MethodSource("containers")
    public void importXml(RedisContainer container) throws Exception {
        execute("import-xml", container);
        RedisKeyCommands<String, String> sync = sync(container);
        List<String> keys = sync.keys("trade:*");
        Assertions.assertEquals(3, keys.size());
        Map<String, String> trade1 = ((RedisHashCommands<String, String>) sync).hgetall("trade:1");
        Assertions.assertEquals("XYZ0001", trade1.get("isin"));
    }


    @ParameterizedTest
    @MethodSource("containers")
    public void exportJSON(RedisContainer container) throws Exception {
        List<DataStructure> records = exportToList(container);
        RedisServerCommands<String, String> sync = sync(container);
        Assertions.assertEquals(sync.dbsize(), records.size());
    }

    @ParameterizedTest
    @MethodSource("containers")
    public void exportJsonGz(RedisContainer container) throws Exception {
        Path file = tempFile("beers.json.gz");
        execute("import-json", container);
        execute("export-json-gz", container, this::configureExportCommand);
        JsonItemReaderBuilder<Map> builder = new JsonItemReaderBuilder<>();
        builder.name("json-file-reader");
        FileSystemResource resource = new FileSystemResource(file);
        builder.resource(new InputStreamResource(new GZIPInputStream(resource.getInputStream()), resource.getDescription()));
        JacksonJsonObjectReader<Map> objectReader = new JacksonJsonObjectReader<>(Map.class);
        objectReader.setMapper(new ObjectMapper());
        builder.jsonObjectReader(objectReader);
        JsonItemReader<Map> reader = builder.build();
        List<Map> records = readAll(reader);
        RedisKeyCommands<String, String> sync = sync(container);
        Assertions.assertEquals(sync.keys("beer:*").size(), records.size());
    }


    private List<DataStructure> exportToList(RedisContainer container) throws Exception {
        Path file = tempFile("redis.json");
        dataGenerator(container).build().call();
        execute("export-json", container, this::configureExportCommand);
        JsonItemReaderBuilder<DataStructure> builder = new JsonItemReaderBuilder<>();
        builder.name("json-data-structure-file-reader");
        builder.resource(new FileSystemResource(file));
        JacksonJsonObjectReader<DataStructure> objectReader = new JacksonJsonObjectReader<>(DataStructure.class);
        objectReader.setMapper(new ObjectMapper());
        builder.jsonObjectReader(objectReader);
        JsonItemReader<DataStructure> reader = builder.build();
        return readAll(reader);
    }

    @ParameterizedTest
    @MethodSource("containers")
    public void exportXml(RedisContainer container) throws Exception {
        dataGenerator(container).build().call();
        Path file = tempFile("redis.xml");
        execute("export-xml", container, this::configureExportCommand);
        XmlItemReaderBuilder<DataStructure> builder = new XmlItemReaderBuilder<>();
        builder.name("xml-file-reader");
        builder.resource(new FileSystemResource(file));
        XmlObjectReader<DataStructure> xmlObjectReader = new XmlObjectReader<>(DataStructure.class);
        xmlObjectReader.setMapper(new XmlMapper());
        builder.xmlObjectReader(xmlObjectReader);
        XmlItemReader<DataStructure<String>> reader = (XmlItemReader) builder.build();
        List<DataStructure<String>> records = readAll(reader);
        RedisServerCommands<String, String> sync = sync(container);
        Assertions.assertEquals(sync.dbsize(), records.size());
        for (DataStructure<String> record : records) {
            String key = record.getKey();
            switch (record.getType().toLowerCase()) {
                case DataStructure.HASH:
                    Assertions.assertEquals(record.getValue(), ((RedisHashCommands<String, String>) sync).hgetall(key));
                    break;
                case DataStructure.STRING:
                    Assertions.assertEquals(record.getValue(), ((RedisStringCommands<String, String>) sync).get(key));
                    break;
            }
        }
    }

    @ParameterizedTest
    @MethodSource("containers")
    public void importJsonAPI(RedisContainer container) throws Exception {
        // riot-file import  hset --keyspace beer --keys id
        FileImportCommand command = new FileImportCommand();
        command.setFiles(Arrays.asList("http://developer.redislabs.com/riot/beers.json"));
        HsetCommand hset = new HsetCommand();
        hset.setKeyspace("beer");
        hset.setKeys(new String[]{"id"});
        command.setRedisCommands(Arrays.asList(hset));
        RiotFile riotFile = new RiotFile();
        configure(riotFile, container);
        command.setApp(riotFile);
        command.call();
        RedisKeyCommands<String, String> sync = sync(container);
        List<String> keys = sync.keys("beer:*");
        Assertions.assertEquals(4432, keys.size());
        Map<String, String> beer1 = ((RedisHashCommands<String, String>) sync).hgetall("beer:1");
        Assertions.assertEquals("Hocus Pocus", beer1.get("name"));
    }

}
