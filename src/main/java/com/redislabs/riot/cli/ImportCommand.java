package com.redislabs.riot.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redislabs.riot.cli.db.DatabaseImportOptions;
import com.redislabs.riot.cli.file.FileImportOptions;
import com.redislabs.riot.cli.file.MapFieldSetMapper;
import com.redislabs.riot.convert.KeyMaker;
import com.redislabs.riot.convert.field.FieldExtractor;
import com.redislabs.riot.convert.map.command.EvalshaArgsProcessor;
import com.redislabs.riot.convert.map.command.HmsetArgsProcessor;
import com.redislabs.riot.convert.map.command.PexpireArgsProcessor;
import com.redislabs.riot.processor.MapFlattener;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.file.DefaultBufferedReaderFactory;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.separator.DefaultRecordSeparatorPolicy;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.batch.item.redis.RedisItemWriter;
import org.springframework.batch.item.redis.support.WriteCommand;
import org.springframework.batch.item.redis.support.commands.*;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.batch.item.xml.XmlItemReader;
import org.springframework.batch.item.xml.XmlObjectReader;
import org.springframework.batch.item.xml.builder.XmlItemReaderBuilder;
import org.springframework.core.convert.converter.Converter;
import org.springframework.jdbc.core.ColumnMapRowMapper;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.util.Assert;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Slf4j
@Command(name = "import", description = "Import data into Redis", sortOptions = false)
public class ImportCommand extends TransferCommand<Map<String, ? extends Object>, Object> {

    public enum CommandName {
        EVALSHA, EXPIRE, GEOADD, FTADD, FTSEARCH, FTAGGREGATE, FTSUGADD, HMSET, LPUSH, NOOP, RPUSH, SADD, SET, XADD, ZADD
    }

    @ArgGroup(exclusive = false, heading = "File options%n")
    private FileImportOptions file = new FileImportOptions();
    @ArgGroup(exclusive = false, heading = "Database options%n")
    private DatabaseImportOptions db = new DatabaseImportOptions();
    @ArgGroup(exclusive = false, heading = "Processor options%n")
    private MapProcessorOptions mapProcessor = new MapProcessorOptions();
    @Option(names = "--command", description = "Redis command: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<name>")
    private CommandName command = CommandName.HMSET;
    @CommandLine.Mixin
    private ScoreOptions score = new ScoreOptions();
    @ArgGroup(exclusive = false, heading = "Collection options%n")
    private MemberOptions member = new MemberOptions();
    @ArgGroup(exclusive = false, heading = "SET options%n")
    private SetOptions set = new SetOptions();
    @ArgGroup(exclusive = false, heading = "Key options%n")
    private KeyOptions key = new KeyOptions();
    @ArgGroup(exclusive = false, heading = "XADD options%n")
    private XaddOptions xadd = new XaddOptions();
    @ArgGroup(exclusive = false, heading = "EVALSHA options%n")
    private EvalshaOptions evalsha = new EvalshaOptions();
    @ArgGroup(exclusive = false, heading = "PEXPIRE options%n")
    private PexpireOptions expire = new PexpireOptions();
    @ArgGroup(exclusive = false, heading = "GEOADD options%n")
    private GeoaddOptions geoadd = new GeoaddOptions();
    @ArgGroup(exclusive = false, heading = "RediSearch options%n")
    private RediSearchOptions search = new RediSearchOptions();

    private KeyMaker<Map<String, String>> keyMaker() {
        return keyMaker(key.getKeyspace(), key.getKeyFields());
    }

    private KeyMaker<Map<String, String>> memberIdMaker() {
        return keyMaker(member.getKeyspace(), member.getFields());
    }

    private KeyMaker<Map<String, String>> keyMaker(String keyspace, String[] fields) {
        return KeyMaker.<Map<String, String>>builder().separator(key.getSeparator()).prefix(keyspace).extractors(fieldExtractors(!key.isKeepKeyFields(), fields)).build();
    }

    private Converter<Map<String, String>, String>[] fieldExtractors(boolean remove, String... fields) {
        List<Converter<Map<String, String>, String>> extractors = new ArrayList<>();
        for (int index = 0; index < fields.length; index++) {
            extractors.add(FieldExtractor.builder().remove(remove).field(fields[index]).build());
        }
        return extractors.toArray(new Converter[0]);
    }

    private WriteCommand<String, String, ? extends Object> writeCommand() {
        switch (command) {
            case EVALSHA:
                return new Evalsha<>(evalsha.getSha(), evalsha.getOutputType());
            case EXPIRE:
                return new Pexpire<>();
            case GEOADD:
                return new Geoadd<>();
            case HMSET:
                return new Hmset<>();
            case LPUSH:
                return new Lpush<>();
            case NOOP:
                return new Noop<>();
            case RPUSH:
                return new Rpush<>();
            case SADD:
                return new Sadd<>();
            case SET:
                return new Set<>();
            case XADD:
                if (xadd.getId() == null) {
                    if (xadd.getMaxlen() == null) {
                        return new Xadd<>();
                    }
                    return new Xadd.XaddMaxlen<>(xadd.getMaxlen(), xadd.isTrim());
                }
                if (xadd.getMaxlen() == null) {
                    new Xadd.XaddId<>();
                }
                return new Xadd.XaddIdMaxlen<>(xadd.getMaxlen(), xadd.isTrim());
            case ZADD:
                return new Zadd<>();
        }
        throw new IllegalArgumentException("Command " + command + " not supported");
    }

    @Override
    protected ItemWriter<Object> writer() {
        return RedisItemWriter.builder().redisOptions(redisOptions()).writeCommand((WriteCommand<String, String, Object>) writeCommand()).build();
    }

    @Override
    protected ItemReader<Map<String, ?>> reader() throws Exception {
        if (db.isSet()) {
            JdbcCursorItemReaderBuilder<Map<String, ?>> builder = new JdbcCursorItemReaderBuilder<>();
            builder.dataSource(db.getDataSource());
            if (db.getFetchSize() != null) {
                builder.fetchSize(db.getFetchSize());
            }
            if (db.getMaxRows() != null) {
                builder.maxRows(db.getMaxRows());
            }
            builder.name("database-reader");
            if (db.getQueryTimeout() != null) {
                builder.queryTimeout(db.getQueryTimeout());
            }
            builder.rowMapper((RowMapper) new ColumnMapRowMapper());
            builder.sql(db.getSql());
            builder.useSharedExtendedConnection(db.isUseSharedExtendedConnection());
            builder.verifyCursorPosition(db.isVerifyCursorPosition());
            JdbcCursorItemReader<Map<String, ?>> reader = builder.build();
            reader.afterPropertiesSet();
            return reader;
        }
        if (file.isSet()) {
            switch (file.getFileType()) {
                case DELIMITED:
                    return (ItemReader) delimitedReader();
                case FIXED:
                    return (ItemReader) fixedLengthReader();
                case JSON:
                    return (ItemReader) jsonReader();
                case XML:
                    return (ItemReader) xmlReader();
            }
        }
        throw new IllegalArgumentException("Unknown import source");
    }

    @Override
    protected ItemProcessor<Map<String, ?>, Object> processor() throws Exception {
        if (db.isSet()) {
            //CompositeItemProcessor
        }
        if (file.isSet()) {
            switch (file.getFileType()) {
                case FIXED:
                case DELIMITED:
                    return (ItemProcessor) mapItemProcessor();
                case JSON:
                case XML:
                    CompositeItemProcessor<Map<String, ?>, Object> processor = new CompositeItemProcessor<>();
                    processor.setDelegates(Arrays.asList(MapFlattener.builder().build(), mapItemProcessor()));
                    return processor;
            }
        }
        throw new IllegalArgumentException("Unknown import source");
    }

    private ItemProcessor<Map<String, String>, ? extends Object> mapItemProcessor() {
        switch (command) {
            case EVALSHA:
                return EvalshaArgsProcessor.builder().keyFields(key.getKeyFields()).argFields(evalsha.getArgs()).build();
            case EXPIRE:
                return PexpireArgsProcessor.builder().keyMaker(keyMaker()).timeoutField(expire.getTimeout()).build();
            case HMSET:
                return HmsetArgsProcessor.builder().keyMaker(keyMaker()).build();
        }
        throw new IllegalArgumentException("Command " + command.name() + " not supported");
    }

    @Override
    protected String taskName() {
        return "Importing";
    }

    private FlatFileItemReaderBuilder<Map<String, String>> flatFileItemReaderBuilder() throws IOException {
        FlatFileItemReaderBuilder<Map<String, String>> builder = new FlatFileItemReaderBuilder<>();
        builder.name("flat-file-reader");
        builder.resource(file.getInputResource());
        builder.encoding(file.getEncoding());
        builder.linesToSkip(file.getLinesToSkip());
        builder.strict(true);
        builder.saveState(false);
        builder.fieldSetMapper(new MapFieldSetMapper());
        builder.recordSeparatorPolicy(new DefaultRecordSeparatorPolicy());
        return builder;
    }

    private ItemReader<Map<String, String>> delimitedReader() throws IOException {
        FlatFileItemReaderBuilder<Map<String, String>> builder = flatFileItemReaderBuilder();
        if (file.isHeader() && file.getLinesToSkip() == 0) {
            builder.linesToSkip(1);
        }
        FlatFileItemReaderBuilder.DelimitedBuilder<Map<String, String>> delimitedBuilder = builder.delimited();
        delimitedBuilder.delimiter(file.getDelimiter());
        delimitedBuilder.includedFields(file.getIncludedFields().toArray(new Integer[0]));
        delimitedBuilder.quoteCharacter(file.getQuoteCharacter());
        String[] fieldNames = file.getNames();
        if (file.isHeader()) {
            BufferedReader reader = new DefaultBufferedReaderFactory().create(file.getInputResource(), file.getEncoding());
            DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
            tokenizer.setDelimiter(file.getDelimiter());
            tokenizer.setQuoteCharacter(file.getQuoteCharacter());
            if (!file.getIncludedFields().isEmpty()) {
                tokenizer.setIncludedFields(file.getIncludedFields().stream().mapToInt(Integer::intValue).toArray());
            }
            fieldNames = tokenizer.tokenize(reader.readLine()).getValues();
            log.debug("Found header {}", Arrays.asList(fieldNames));
        }
        if (fieldNames == null || fieldNames.length == 0) {
            throw new IOException("No fields specified");
        }
        delimitedBuilder.names(fieldNames);
        return builder.build();
    }

    private AbstractItemCountingItemStreamItemReader<Map<String, String>> fixedLengthReader() throws IOException {
        FlatFileItemReaderBuilder<Map<String, String>> builder = flatFileItemReaderBuilder();
        FlatFileItemReaderBuilder.FixedLengthBuilder<Map<String, String>> fixedlength = builder.fixedLength();
        Assert.notEmpty(file.getColumnRanges(), "Column ranges are required");
        fixedlength.columns(file.getColumnRanges().toArray(new Range[file.getColumnRanges().size()]));
        fixedlength.names(file.getNames());
        return builder.build();
    }

    private JsonItemReader<Map> jsonReader() throws IOException {
        JsonItemReaderBuilder<Map> builder = new JsonItemReaderBuilder<>();
        builder.name("json-file-reader");
        builder.resource(file.getInputResource());
        JacksonJsonObjectReader<Map> objectReader = new JacksonJsonObjectReader<>(Map.class);
        objectReader.setMapper(new ObjectMapper());
        builder.jsonObjectReader(objectReader);
        return builder.build();
    }

    private XmlItemReader<Map> xmlReader() throws IOException {
        XmlItemReaderBuilder<Map> builder = new XmlItemReaderBuilder<>();
        builder.name("xml-file-reader");
        builder.resource(file.getInputResource());
        XmlObjectReader<Map> objectReader = new XmlObjectReader<>(Map.class);
        objectReader.setMapper(new XmlMapper());
        builder.xmlObjectReader(objectReader);
        return builder.build();
    }

    private void temp() {
//        switch (format) {
//            case RAW:
//                return SetField.builder().field(value).build();
//            case XML:
//                return SetObject.builder().objectWriter(new XmlMapper().writer().withRootName(root)).build();
//            default:
//                return SetObject.builder().objectWriter(new ObjectMapper().writer().withRootName(root)).build();
//        }
//        CommandWriter<Map<String, Object>> commandWriter = mapCommandWriter(command);
//        if (commandWriter instanceof AbstractKeyMapCommandWriter) {
//            AbstractKeyMapCommandWriter keyWriter = (AbstractKeyMapCommandWriter) commandWriter;
//            if (keyspace == null && keys.length == 0) {
//                log.warn("No keyspace nor key fields specified; using empty key (\"\")");
//            }
//            keyWriter.setKeyBuilder(KeyBuilder.builder().separator(separator).prefix(keyspace).fields(keys).build());
//            keyWriter.setKeepKeyFields(keepKeyFields);
//            if (commandWriter instanceof AbstractCollectionMapCommandWriter) {
//                ((AbstractCollectionMapCommandWriter) commandWriter).setMemberIdBuilder(memberIdBuilder());
//            }
//        }
//        return redisItemWriter(commandWriter, timeout);
    }

}
