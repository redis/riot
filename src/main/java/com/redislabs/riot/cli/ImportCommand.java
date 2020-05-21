package com.redislabs.riot.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.RediSearchCommands;
import com.redislabs.lettusearch.RediSearchUtils;
import com.redislabs.lettusearch.index.IndexInfo;
import com.redislabs.lettusearch.index.field.Field;
import com.redislabs.lettusearch.index.field.GeoField;
import com.redislabs.lettusearch.index.field.TagField;
import com.redislabs.lettusearch.index.field.TextField;
import com.redislabs.lettusearch.search.AddOptions;
import com.redislabs.riot.cli.db.DatabaseImportOptions;
import com.redislabs.riot.cli.file.FileImportOptions;
import com.redislabs.riot.cli.file.FileType;
import com.redislabs.riot.cli.file.GZIPInputStreamResource;
import com.redislabs.riot.cli.file.MapFieldSetMapper;
import com.redislabs.riot.convert.KeyMaker;
import com.redislabs.riot.convert.ObjectMapperConverter;
import com.redislabs.riot.convert.field.FieldExtractor;
import com.redislabs.riot.convert.field.MapToArrayConverter;
import com.redislabs.riot.file.StandardInputResource;
import com.redislabs.riot.generator.GeneratorReader;
import com.redislabs.riot.processor.MapFlattener;
import com.redislabs.riot.processor.ObjectMapToStringMapProcessor;
import com.redislabs.riot.processor.command.*;
import com.redislabs.riot.transfer.Transfer;
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
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.batch.item.redis.RedisItemWriter;
import org.springframework.batch.item.redis.support.RedisOptions;
import org.springframework.batch.item.redis.support.WriteCommand;
import org.springframework.batch.item.redis.support.commands.Set;
import org.springframework.batch.item.redis.support.commands.*;
import org.springframework.batch.item.redisearch.RediSearchDocumentItemWriter;
import org.springframework.batch.item.redisearch.RediSearchSuggestItemWriter;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.batch.item.support.PassThroughItemProcessor;
import org.springframework.batch.item.xml.XmlObjectReader;
import org.springframework.batch.item.xml.builder.XmlItemReaderBuilder;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.ColumnMapRowMapper;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.util.Assert;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;

@Slf4j
@Command(name = "import", description = "Import data into Redis", sortOptions = false, subcommands = FakerHelpCommand.class)
public class ImportCommand extends TransferCommand<Map<String, Object>, Object> {

    public enum CommandName {
        EVALSHA, EXPIRE, GEOADD, FTADD, FTSEARCH, FTAGGREGATE, FTSUGADD, HMSET, LPUSH, NOOP, RPUSH, SADD, SET, XADD, ZADD
    }

    @ArgGroup(exclusive = false, heading = "File options%n")
    private final FileImportOptions file = new FileImportOptions();
    @ArgGroup(exclusive = false, heading = "Database options%n")
    private final DatabaseImportOptions db = new DatabaseImportOptions();
    @ArgGroup(exclusive = false, heading = "Generator options%n")
    private final GeneratorOptions gen = new GeneratorOptions();
    @ArgGroup(exclusive = false, heading = "Processor options%n")
    private final MapProcessorOptions mapProcessor = new MapProcessorOptions();
    @Option(names = "--command", description = "Redis command: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<name>")
    private CommandName command = CommandName.HMSET;
    @CommandLine.Option(names = "--remove-fields", description = "Remove fields already used in data structures")
    private boolean removeFields;
    @ArgGroup(exclusive = false, heading = "Score options%n")
    private final ScoreOptions score = new ScoreOptions();
    @ArgGroup(exclusive = false, heading = "Collection options%n")
    private final MemberOptions member = new MemberOptions();
    @ArgGroup(exclusive = false, heading = "SET options%n")
    private final SetOptions set = new SetOptions();
    @ArgGroup(exclusive = false, heading = "Key options%n")
    private final KeyOptions key = new KeyOptions();
    @ArgGroup(exclusive = false, heading = "XADD options%n")
    private final XaddOptions xadd = new XaddOptions();
    @ArgGroup(exclusive = false, heading = "EVALSHA options%n")
    private final EvalshaOptions evalsha = new EvalshaOptions();
    @ArgGroup(exclusive = false, heading = "PEXPIRE options%n")
    private final PexpireOptions expire = new PexpireOptions();
    @ArgGroup(exclusive = false, heading = "GEOADD options%n")
    private final GeoaddOptions geoadd = new GeoaddOptions();
    @ArgGroup(exclusive = false, heading = "FT options%n")
    private final FtOptions ft = new FtOptions();
    @ArgGroup(exclusive = false, heading = "FT.ADD options%n")
    private final FtAddOptions ftAdd = new FtAddOptions();
    @ArgGroup(exclusive = false, heading = "FT.SUGADD options%n")
    private final FtSugaddOptions ftSugadd = new FtSugaddOptions();

    private KeyMaker<Map<String, String>> keyMaker() {
        return idMaker(key.getKeyspace(), key.getKeyFields());
    }

    private KeyMaker<Map<String, String>> memberIdMaker() {
        return idMaker(member.getKeyspace(), member.getFields());
    }

    private KeyMaker<Map<String, String>> idMaker(String prefix, String[] fields) {
        return KeyMaker.<Map<String, String>>builder().separator(key.getSeparator()).prefix(prefix).extractors(fieldExtractors(removeFields, fields)).build();
    }

    private Converter<Map<String, String>, String>[] fieldExtractors(boolean remove, String... fields) {
        List<Converter<Map<String, String>, String>> extractors = new ArrayList<>();
        for (String field : fields) {
            extractors.add(FieldExtractor.builder().remove(remove).field(field).build());
        }
        return extractors.toArray(new Converter[0]);
    }

    private WriteCommand<String, String, ?> writeCommand() {
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
                if (xadd.getIdField() == null) {
                    if (xadd.getMaxlen() == null) {
                        return new Xadd<>();
                    }
                    return new Xadd.XaddMaxlen<>(xadd.getMaxlen(), xadd.isApproximateTrimming());
                }
                if (xadd.getMaxlen() == null) {
                    new Xadd.XaddId<>();
                }
                return new Xadd.XaddIdMaxlen<>(xadd.getMaxlen(), xadd.isApproximateTrimming());
            case ZADD:
                return new Zadd<>();
        }
        throw new IllegalArgumentException("Command " + command + " not supported");
    }

    @Override
    protected Transfer<Map<String, Object>, Object> getTransfer() throws Exception {
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
            return getObjectMapTransfer(reader);
        }
        if (file.isSet()) {
            FileType fileType = file.getFileType();
            Resource resource = resource();
            switch (fileType) {
                case DELIMITED:
                    FlatFileItemReaderBuilder delimitedReaderBuilder = flatFileReaderBuilder(resource);
                    FlatFileItemReaderBuilder.DelimitedBuilder delimitedBuilder = delimitedReaderBuilder.delimited();
                    delimitedBuilder.delimiter(file.getDelimiter());
                    delimitedBuilder.includedFields(file.getIncludedFields().toArray(new Integer[0]));
                    delimitedBuilder.quoteCharacter(file.getQuoteCharacter());
                    String[] fieldNames = file.getNames();
                    if (file.isHeader()) {
                        BufferedReader reader = new DefaultBufferedReaderFactory().create(resource, file.getEncoding());
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
                    return getStringMapTransfer(delimitedReaderBuilder.build());
                case FIXED:
                    FlatFileItemReaderBuilder fixedReaderBuilder = flatFileReaderBuilder(resource);
                    FlatFileItemReaderBuilder.FixedLengthBuilder fixedLength = fixedReaderBuilder.fixedLength();
                    Assert.notEmpty(file.getColumnRanges(), "Column ranges are required");
                    fixedLength.columns(file.getColumnRanges().toArray(new Range[0]));
                    fixedLength.names(file.getNames());
                    return getStringMapTransfer(fixedReaderBuilder.build());
                case JSON:
                    JsonItemReaderBuilder<Map> jsonReaderBuilder = new JsonItemReaderBuilder<>();
                    jsonReaderBuilder.name("json-file-reader");
                    jsonReaderBuilder.resource(resource());
                    JacksonJsonObjectReader<Map> jsonObjectReader = new JacksonJsonObjectReader<>(Map.class);
                    jsonObjectReader.setMapper(new ObjectMapper());
                    jsonReaderBuilder.jsonObjectReader(jsonObjectReader);
                    return getNestedMapTransfer(jsonReaderBuilder.build());
                case XML:
                    XmlItemReaderBuilder<Map> xmlReaderBuilder = new XmlItemReaderBuilder<>();
                    xmlReaderBuilder.name("xml-file-reader");
                    xmlReaderBuilder.resource(resource());
                    XmlObjectReader<Map> xmlObjectReader = new XmlObjectReader<>(Map.class);
                    xmlObjectReader.setMapper(new XmlMapper());
                    xmlReaderBuilder.xmlObjectReader(xmlObjectReader);
                    return getNestedMapTransfer(xmlReaderBuilder.build());
            }
        }
        if (gen.isSet()) {
            return getObjectMapTransfer(GeneratorReader.builder().locale(gen.getLocale()).includeMetadata(gen.isIncludeMetadata()).fields(fakerFields()).build());
        }
        throw new IllegalArgumentException("Unknown import source");
    }

    private FlatFileItemReaderBuilder flatFileReaderBuilder(Resource resource) {
        FlatFileItemReaderBuilder<Map<String, String>> flatFileReaderBuilder = new FlatFileItemReaderBuilder<>();
        flatFileReaderBuilder.name("flat-file-reader");
        flatFileReaderBuilder.resource(resource);
        flatFileReaderBuilder.encoding(file.getEncoding());
        flatFileReaderBuilder.linesToSkip(file.getLinesToSkip());
        flatFileReaderBuilder.strict(true);
        flatFileReaderBuilder.saveState(false);
        flatFileReaderBuilder.fieldSetMapper(new MapFieldSetMapper());
        flatFileReaderBuilder.recordSeparatorPolicy(new DefaultRecordSeparatorPolicy());
        if (file.isHeader() && file.getLinesToSkip() == 0) {
            flatFileReaderBuilder.linesToSkip(1);
        }
        return flatFileReaderBuilder;
    }

    private Resource resource() throws IOException {
        if (file.isConsoleResource()) {
            return new StandardInputResource();
        }
        Resource resource = file.getResource();
        if (file.isGzip()) {
            return new GZIPInputStreamResource(resource.getInputStream(), resource.getDescription());
        }
        return resource;

    }

    protected Transfer<Map<String, Object>, Object> getObjectMapTransfer(ItemReader reader) {
        CompositeItemProcessor processor = new CompositeItemProcessor();
        processor.setDelegates(Arrays.asList(ObjectMapToStringMapProcessor.builder().build(), commandArgsProcessor()));
        return getTransfer(reader, processor);
    }

    protected Transfer<Map<String, Object>, Object> getStringMapTransfer(ItemReader reader) {
        return getTransfer(reader, commandArgsProcessor());
    }

    private Transfer<Map<String, Object>, Object> getNestedMapTransfer(ItemReader reader) {
        CompositeItemProcessor processor = new CompositeItemProcessor();
        processor.setDelegates(Arrays.asList(MapFlattener.builder().build(), commandArgsProcessor()));
        return getTransfer(reader, processor);
    }

    private Transfer<Map<String, Object>, Object> getTransfer(ItemReader reader, ItemProcessor processor) {
        return new Transfer<>(reader, processor, writer());
    }

    private ItemWriter<?> writer() {
        switch (command) {
            case FTADD:
                AddOptions addOptions = AddOptions.builder().ifCondition(ftAdd.getIfCondition()).language(ftAdd.getLanguage()).noSave(ftAdd.isNosave()).replace(ftAdd.isReplace()).replacePartial(ftAdd.isReplacePartial()).build();
                return RediSearchDocumentItemWriter.builder().rediSearchOptions(rediSearchOptions()).index(ft.getIndex()).addOptions(addOptions).build();
            case FTSUGADD:
                return RediSearchSuggestItemWriter.builder().rediSearchOptions(rediSearchOptions()).key(ft.getIndex()).increment(ftSugadd.isIncrement()).build();
        }
        return RedisItemWriter.builder().redisOptions(redisOptions()).writeCommand((WriteCommand<String, String, Object>) writeCommand()).build();
    }

    protected ItemProcessor<Map<String, String>, ?> commandArgsProcessor() {
        switch (command) {
            case EVALSHA:
                return new EvalshaArgsProcessor<>(MapToArrayConverter.builder().fields(key.getKeyFields()).build(), MapToArrayConverter.builder().fields(evalsha.getArgs()).build());
            case EXPIRE:
                return new PexpireArgsProcessor<>(keyMaker(), longFieldExtractor(expire.getTimeout()));
            case HMSET:
                return new HmsetArgsProcessor<>(keyMaker());
            case GEOADD:
                return new GeoaddArgsProcessor<>(keyMaker(), memberIdMaker(), doubleFieldExtractor(geoadd.getLongitudeField()), doubleFieldExtractor(geoadd.getLatitudeField()));
            case LPUSH:
            case RPUSH:
            case SADD:
                return new MemberArgsProcessor<>(keyMaker(), memberIdMaker());
            case SET:
                return new SetArgsProcessor<>(keyMaker(), stringValueConverter());
            case NOOP:
                return new PassThroughItemProcessor<>();
            case XADD:
                return new XaddArgsProcessor<>(keyMaker(), fieldExtractor(xadd.getIdField()));
            case ZADD:
                return new ZaddArgsProcessor<>(keyMaker(), memberIdMaker(), scoreConverter());
            case FTADD:
                return new DocumentProcessor<>(keyMaker(), scoreConverter(), fieldExtractor(ft.getPayloadField()));
            case FTSUGADD:
                return new SuggestionProcessor<>(fieldExtractor(ftSugadd.getField()), scoreConverter(), fieldExtractor(ft.getPayloadField()));
        }
        throw new IllegalArgumentException("Command " + command.name() + " not supported");
    }

    private Converter<Map<String, String>, String> fieldExtractor(String field) {
        return fieldExtractor(field, String.class);
    }

    private Converter<Map<String, String>, Long> longFieldExtractor(String field) {
        return fieldExtractor(field, Long.class);
    }

    private Converter<Map<String, String>, Double> doubleFieldExtractor(String field) {
        return fieldExtractor(field, Double.class);
    }


    private <T> Converter<Map<String, String>, T> fieldExtractor(String field, Class<T> type) {
        return FieldExtractor.builder(type).field(field).remove(removeFields).build();
    }


    private Converter<Map<String, String>, Double> scoreConverter() {
        return FieldExtractor.builder(Double.class).field(score.getField()).defaultValue(score.getDefaultValue()).remove(removeFields).build();
    }

    private Converter<Map<String, String>, String> stringValueConverter() {
        switch (set.getFormat()) {
            case RAW:
                return fieldExtractor(set.getValueField());
            case XML:
                return new ObjectMapperConverter<>(new XmlMapper().writer().withRootName(set.getRoot()));
            case JSON:
                return new ObjectMapperConverter<>(new ObjectMapper().writer().withRootName(set.getRoot()));
        }
        throw new IllegalArgumentException("Unsupported String format: " + set.getFormat());
    }

    private String expression(Field field) {
        if (field instanceof TextField) {
            return "lorem.paragraph";
        }
        if (field instanceof TagField) {
            return "number.digits(10)";
        }
        if (field instanceof GeoField) {
            return "address.longitude.concat(',').concat(address.latitude)";
        }
        return "number.randomDouble(3,-1000,1000)";
    }

    private String quotes(String field, String expression) {
        return "\"" + field + "=" + expression + "\"";
    }

    private List<String> fakerArgs(Map<String, String> fakerFields) {
        List<String> args = new ArrayList<>();
        fakerFields.forEach((k, v) -> args.add(quotes(k, v)));
        return args;
    }

    private Map<String, String> fakerFields() {
        Map<String, String> fields = new LinkedHashMap<>(gen.getFakerFields());
        if (gen.getFakerIndex() == null) {
            return fields;
        }
        RedisOptions redisOptions = redisOptions();
        RediSearchClient rediSearchClient = redisOptions.getClientResources() == null ? RediSearchClient.create(redisOptions.getRedisURI()) : RediSearchClient.create(redisOptions.getClientResources(), redisOptions.getRedisURI());
        RediSearchCommands<String, String> commands = rediSearchClient.connect().sync();
        IndexInfo info = RediSearchUtils.getInfo(commands.ftInfo(gen.getFakerIndex()));
        for (Field field : info.getFields()) {
            fields.put(field.getName(), expression(field));
        }
        log.info("Introspected fields: {}", String.join(" ", fakerArgs(fields)));
        return fields;
    }


    @Override
    protected String taskName() {
        return "Importing";
    }

}
