package com.redislabs.riot;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
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
import org.springframework.batch.item.redis.support.CommandItemWriters.*;
import org.springframework.batch.item.redisearch.RediSearchItemWriter;
import org.springframework.batch.item.redisearch.RediSearchSuggestItemWriter;
import org.springframework.batch.item.redisearch.support.DocumentItemProcessor;
import org.springframework.batch.item.redisearch.support.SuggestionItemProcessor;
import org.springframework.core.convert.converter.Converter;
import picocli.CommandLine;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings({"rawtypes", "unchecked"})
public abstract class AbstractImportCommand<I> extends AbstractTransferCommand<I, Object> {

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
    @CommandLine.Option(names = "--remove-fields", description = "Remove fields already used in data structures")
    private boolean removeFields;
    @Getter
    @CommandLine.Option(names = "--command", description = "Redis command: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<name>")
    private CommandName command = CommandName.HMSET;
    @CommandLine.ArgGroup(exclusive = false, heading = "Redis command options%n")
    private final RedisImportOptions redis = new RedisImportOptions();
    @CommandLine.ArgGroup(exclusive = false, heading = "RediSearch command options%n")
    private final RediSearchImportOptions redisearch = new RediSearchImportOptions();

    @Override
    protected String taskName() {
        return "Importing";
    }

    public SpelProcessor spelProcessor() {
        return configure(SpelProcessor.builder().dateFormat(new SimpleDateFormat(dateFormat)).variables(variables).fields(spel)).build();
    }

    private KeyMaker<Map<String, String>> keyMaker() {
        return idMaker(redis.getKeyspace(), redis.getKeyFields());
    }

    private KeyMaker<Map<String, String>> memberIdMaker() {
        return idMaker(redis.getMemberSpace(), redis.getMemberFields());
    }

    private KeyMaker<Map<String, String>> idMaker(String prefix, String[] fields) {
        return KeyMaker.<Map<String, String>>builder().separator(redis.getKeySeparator()).prefix(prefix).extractors(fieldExtractors(removeFields, fields)).build();
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
        return compositeProcessor(allProcessors);
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
        switch (command) {
            case EVALSHA:
                return configure(Eval.<Map<String, String>>builder().sha(redis.getEvalSha()).outputType(redis.getEvalOutputType()).keysConverter(MapToArrayConverter.builder().fields(redis.getKeyFields()).build()).argsConverter(MapToArrayConverter.builder().fields(redis.getEvalArgs()).build())).build();
            case EXPIRE:
                return configureKeyCommandWriterBuilder(Expire.<Map<String, String>>builder().timeoutConverter(longFieldExtractor(redis.getTimeout()))).build();
            case HMSET:
                return configureKeyCommandWriterBuilder(Hmset.<Map<String, String>>builder().mapConverter(new IdemConverter<>())).build();
            case GEOADD:
                return configureCollectionCommandWriterBuilder(Geoadd.<Map<String, String>>builder().longitudeConverter(doubleFieldExtractor(redis.getLongitudeField())).latitudeConverter(doubleFieldExtractor(redis.getLatitudeField()))).build();
            case LPUSH:
                return configureCollectionCommandWriterBuilder(Lpush.builder()).build();
            case RPUSH:
                return configureCollectionCommandWriterBuilder(Rpush.builder()).build();
            case SADD:
                return configureCollectionCommandWriterBuilder(Sadd.builder()).build();
            case SET:
                return configureKeyCommandWriterBuilder(Set.<Map<String, String>>builder().valueConverter(stringValueConverter())).build();
            case NOOP:
                return configure(Noop.<Map<String, String>>builder()).build();
            case XADD:
                return configureKeyCommandWriterBuilder(Xadd.<Map<String, String>>builder().bodyConverter(new IdemConverter<>()).idConverter(fieldExtractor(redis.getIdField())).maxlen(redis.getMaxlen()).approximateTrimming(redis.isApproximateTrimming())).build();
            case ZADD:
                return configureCollectionCommandWriterBuilder(Zadd.<Map<String, String>>builder().scoreConverter(scoreConverter())).build();
            case FTADD:
                AddOptions addOptions = AddOptions.builder().ifCondition(redisearch.getIfCondition()).language(redisearch.getLanguage()).noSave(redisearch.isNosave()).replace(redisearch.isReplace()).replacePartial(redisearch.isReplacePartial()).build();
                return configure(RediSearchItemWriter.builder().index(redisearch.getIndex()).addOptions(addOptions)).build();
            case FTSUGADD:
                return configure(RediSearchSuggestItemWriter.builder().increment(redisearch.isIncrement()).key(redisearch.getIndex())).build();
        }
        throw new IllegalArgumentException("Command not supported");
    }

    private <B extends KeyCommandWriterBuilder<B, Map<String, String>>> B configureKeyCommandWriterBuilder(B builder) {
        return configure(builder.keyConverter(keyMaker()));
    }

    private <B extends CollectionCommandWriterBuilder<B, Map<String, String>>> B configureCollectionCommandWriterBuilder(B builder) {
        return configureKeyCommandWriterBuilder(builder.keyConverter(keyMaker()).memberIdConverter(memberIdMaker()));
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
        return FieldExtractor.builder(Double.class).field(redis.getField()).defaultValue(redis.getDefaultValue()).remove(removeFields).build();
    }

    private Converter<Map<String, String>, String> stringValueConverter() {
        switch (redis.getFormat()) {
            case RAW:
                return fieldExtractor(redis.getValueField());
            case XML:
                return new ObjectMapperConverter<>(new XmlMapper().writer().withRootName(redis.getRoot()));
            case JSON:
                return new ObjectMapperConverter<>(new ObjectMapper().writer().withRootName(redis.getRoot()));
        }
        throw new IllegalArgumentException("Unsupported String format: " + redis.getFormat());
    }


}
