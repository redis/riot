package com.redislabs.riot.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redislabs.lettuce.helper.RedisOptions;
import com.redislabs.lettusearch.search.AddOptions;
import com.redislabs.riot.convert.IdemConverter;
import com.redislabs.riot.convert.KeyMaker;
import com.redislabs.riot.convert.ObjectMapperConverter;
import com.redislabs.riot.convert.field.FieldExtractor;
import com.redislabs.riot.convert.field.MapToArrayConverter;
import com.redislabs.riot.processor.ObjectMapToStringMapProcessor;
import com.redislabs.riot.processor.SpelProcessor;
import lombok.Getter;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.support.RedisCommandItemWriters;
import org.springframework.batch.item.redisearch.RediSearchItemWriter;
import org.springframework.batch.item.redisearch.RediSearchSuggestItemWriter;
import org.springframework.batch.item.redisearch.support.DocumentItemProcessor;
import org.springframework.batch.item.redisearch.support.SuggestionItemProcessor;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.batch.item.support.PassThroughItemProcessor;
import org.springframework.core.convert.converter.Converter;
import picocli.CommandLine;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@CommandLine.Command(name = "import", description = "Import data into Redis", subcommands = {FileImportCommand.class, DatabaseImportCommand.class, GenerateCommand.class}, sortOptions = false)
public class ImportCommand extends TransferCommand {

    public enum CommandName {
        EVALSHA, EXPIRE, GEOADD, FTADD, FTSEARCH, FTAGGREGATE, FTSUGADD, HMSET, LPUSH, NOOP, RPUSH, SADD, SET, XADD, ZADD
    }

    @Getter
    @CommandLine.Option(arity = "1..*", names = "--spel", description = "SpEL expression to produce a field", paramLabel = "<field=exp>")
    private Map<String, String> spel = new HashMap<>();
    @CommandLine.Option(arity = "1..*", names = "--spel-var", description = "Register a variable in the SpEL processor context", paramLabel = "<v=exp>")
    private Map<String, String> variables = new HashMap<>();
    @CommandLine.Option(names = "--date-format", description = "Processor date format (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
    private String dateFormat = new SimpleDateFormat().toPattern();
    @Getter
    @CommandLine.Option(names = "--command", description = "Redis command: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<name>")
    private CommandName command = CommandName.HMSET;
    @CommandLine.Option(names = "--key-separator", description = "Key separator (default: ${DEFAULT-VALUE})", paramLabel = "<str>")
    private String separator = KeyMaker.DEFAULT_SEPARATOR;
    @CommandLine.Option(names = {"-p", "--keyspace"}, description = "Keyspace prefix", paramLabel = "<str>")
    private String keyspace;
    @CommandLine.Option(names = {"-k", "--keys"}, arity = "1..*", description = "Key fields", paramLabel = "<fields>")
    private String[] keyFields = new String[0];
    @CommandLine.Option(names = "--remove-fields", description = "Remove fields already used in data structures")
    private boolean removeFields;
    @CommandLine.Option(names = "--member-space", description = "Prefix for member IDs", paramLabel = "<str>")
    private String memberSpace;
    @CommandLine.Option(names = "--members", arity = "1..*", description = "Member field names for collections", paramLabel = "<fields>")
    private String[] memberFields = new String[0];
    @CommandLine.ArgGroup(exclusive = false, heading = "Redis command options%n")
    private final RedisCommandOptions commandOptions = new RedisCommandOptions();
    @CommandLine.ArgGroup(exclusive = false, heading = "RediSearch command options%n")
    private final RediSearchCommandOptions redisearch = new RediSearchCommandOptions();

    @Override
    protected String taskName() {
        return "Importing";
    }

    public SpelProcessor spelProcessor() {
        RedisOptions redisOptions = redisOptions();
        return SpelProcessor.builder().connection(redisOptions.connection()).commands(redisOptions.sync()).dateFormat(new SimpleDateFormat(dateFormat)).variables(variables).fields(spel).build();
    }

    private KeyMaker<Map<String, String>> keyMaker() {
        return idMaker(keyspace, keyFields);
    }

    private KeyMaker<Map<String, String>> memberIdMaker() {
        return idMaker(memberSpace, memberFields);
    }

    private KeyMaker<Map<String, String>> idMaker(String prefix, String[] fields) {
        return KeyMaker.<Map<String, String>>builder().separator(separator).prefix(prefix).extractors(fieldExtractors(removeFields, fields)).build();
    }

    private Converter<Map<String, String>, String>[] fieldExtractors(boolean remove, String... fields) {
        List<Converter<Map<String, String>, String>> extractors = new ArrayList<>();
        for (String field : fields) {
            extractors.add(FieldExtractor.builder().remove(remove).field(field).build());
        }
        return extractors.toArray(new Converter[0]);
    }

    public ItemProcessor objectMapProcessor() {
        List<ItemProcessor> processors = new ArrayList<>();
        if (!spel.isEmpty()) {
            processors.add(spelProcessor());
        }
        processors.add(ObjectMapToStringMapProcessor.builder().build());
        return processor(processors);
    }

    public ItemProcessor processor(List<ItemProcessor> processors) {
        List<ItemProcessor> allProcessors = new ArrayList<>(processors);
        switch (command) {
            case FTADD:
                allProcessors.add(documentItemProcessor());
                break;
            case FTSUGADD:
                allProcessors.add(suggestionItemProcessor());
                break;
        }
        if (allProcessors.isEmpty()) {
            return new PassThroughItemProcessor();
        }
        if (allProcessors.size() == 1) {
            return allProcessors.get(0);
        }
        CompositeItemProcessor compositeItemProcessor = new CompositeItemProcessor();
        compositeItemProcessor.setDelegates(allProcessors);
        return compositeItemProcessor;
    }

    private ItemProcessor suggestionItemProcessor() {
        SuggestionItemProcessor.SuggestionItemProcessorBuilder<String, String> builder = SuggestionItemProcessor.<String, String>builder().stringConverter(fieldExtractor(redisearch.getField())).scoreConverter(scoreConverter());
        if (redisearch.getPayloadField() != null) {
            builder.payloadConverter(fieldExtractor(redisearch.getPayloadField()));
        }
        return builder.build();
    }

    private ItemProcessor documentItemProcessor() {
        DocumentItemProcessor.DocumentItemProcessorBuilder<String, String> builder = DocumentItemProcessor.<String, String>builder().idConverter(keyMaker()).scoreConverter(scoreConverter());
        if (redisearch.getPayloadField() != null) {
            builder.payloadConverter(fieldExtractor(redisearch.getPayloadField()));
        }
        return builder.build();
    }

    public ItemWriter writer() {
        RedisOptions redisOptions = redisOptions();
        switch (command) {
            case EVALSHA:
                return new RedisCommandItemWriters.Eval<>(redisOptions.connectionPool(), redisOptions.async(), redisOptions.getTimeout(), commandOptions.getEvalSha(), commandOptions.getEvalOutputType(), MapToArrayConverter.builder().fields(keyFields).build(), MapToArrayConverter.builder().fields(commandOptions.getEvalArgs()).build());
            case EXPIRE:
                return new RedisCommandItemWriters.Expire(redisOptions.connectionPool(), redisOptions.async(), redisOptions.getTimeout(), keyMaker(), longFieldExtractor(commandOptions.getTimeout()));
            case HMSET:
                return new RedisCommandItemWriters.Hmset(redisOptions.connectionPool(), redisOptions.async(), redisOptions.getTimeout(), keyMaker(), new IdemConverter<>());
            case GEOADD:
                return new RedisCommandItemWriters.Geoadd(redisOptions.connectionPool(), redisOptions.async(), redisOptions.getTimeout(), keyMaker(), memberIdMaker(), doubleFieldExtractor(commandOptions.getLongitudeField()), doubleFieldExtractor(commandOptions.getLatitudeField()));
            case LPUSH:
                return new RedisCommandItemWriters.Lpush(redisOptions.connectionPool(), redisOptions.async(), redisOptions.getTimeout(), keyMaker(), memberIdMaker());
            case RPUSH:
                return new RedisCommandItemWriters.Rpush(redisOptions.connectionPool(), redisOptions.async(), redisOptions.getTimeout(), keyMaker(), memberIdMaker());
            case SADD:
                return new RedisCommandItemWriters.Sadd(redisOptions.connectionPool(), redisOptions.async(), redisOptions.getTimeout(), keyMaker(), memberIdMaker());
            case SET:
                return new RedisCommandItemWriters.Set(redisOptions.connectionPool(), redisOptions.async(), redisOptions.getTimeout(), keyMaker(), stringValueConverter());
            case NOOP:
                return new RedisCommandItemWriters.Noop<>(redisOptions.connectionPool(), redisOptions.async(), redisOptions.getTimeout());
            case XADD:
                return new RedisCommandItemWriters.Xadd(redisOptions.connectionPool(), redisOptions.async(), redisOptions.getTimeout(), keyMaker(), new IdemConverter<>(), fieldExtractor(commandOptions.getIdField()), commandOptions.getMaxlen(), commandOptions.isApproximateTrimming());
            case ZADD:
                return new RedisCommandItemWriters.Zadd(redisOptions.connectionPool(), redisOptions.async(), redisOptions.getTimeout(), keyMaker(), memberIdMaker(), scoreConverter());
            case FTADD:
                return searchWriter();
            case FTSUGADD:
                return suggestWriter();
        }
        throw new IllegalArgumentException("Command not supported");
    }

    private RediSearchItemWriter<String, String> searchWriter() {
        AddOptions addOptions = AddOptions.builder().ifCondition(redisearch.getIfCondition()).language(redisearch.getLanguage()).noSave(redisearch.isNosave()).replace(redisearch.isReplace()).replacePartial(redisearch.isReplacePartial()).build();
        return RediSearchItemWriter.builder().redisOptions(redisOptions()).index(redisearch.getIndex()).addOptions(addOptions).build();
    }

    private RediSearchSuggestItemWriter<String, String> suggestWriter() {
        return RediSearchSuggestItemWriter.builder().redisOptions(redisOptions()).increment(redisearch.isIncrement()).key(redisearch.getIndex()).build();
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
        return FieldExtractor.builder(Double.class).field(commandOptions.getField()).defaultValue(commandOptions.getDefaultValue()).remove(removeFields).build();
    }

    private Converter<Map<String, String>, String> stringValueConverter() {
        switch (commandOptions.getFormat()) {
            case RAW:
                return fieldExtractor(commandOptions.getValueField());
            case XML:
                return new ObjectMapperConverter<>(new XmlMapper().writer().withRootName(commandOptions.getRoot()));
            case JSON:
                return new ObjectMapperConverter<>(new ObjectMapper().writer().withRootName(commandOptions.getRoot()));
        }
        throw new IllegalArgumentException("Unsupported String format: " + commandOptions.getFormat());
    }


}
