package com.redislabs.riot.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.search.AddOptions;
import com.redislabs.picocliredis.RedisOptions;
import com.redislabs.riot.convert.IdemConverter;
import com.redislabs.riot.convert.KeyMaker;
import com.redislabs.riot.convert.ObjectMapperConverter;
import com.redislabs.riot.convert.field.FieldExtractor;
import com.redislabs.riot.convert.field.MapToArrayConverter;
import com.redislabs.riot.processor.ObjectMapToStringMapProcessor;
import com.redislabs.riot.processor.SpelProcessor;
import com.redislabs.riot.processor.command.DocumentItemProcessor;
import com.redislabs.riot.processor.command.PayloadDocumentItemProcessor;
import com.redislabs.riot.processor.command.PayloadSuggestionItemProcessor;
import com.redislabs.riot.processor.command.SuggestionItemProcessor;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import lombok.Getter;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.support.RedisClusterDataStructureItemWriters;
import org.springframework.batch.item.redis.support.RedisDataStructureItemWriters;
import org.springframework.batch.item.redisearch.RediSearchDocumentItemWriter;
import org.springframework.batch.item.redisearch.RediSearchSuggestItemWriter;
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


    public SpelProcessor spelProcessor() {
        RedisOptions redisOptions = getRedisOptions();
        return SpelProcessor.builder().connection(redisOptions.isCluster() ? redisOptions.redisClusterClient().connect() : redisOptions.redisClient().connect()).commands(redisOptions.isCluster() ? c -> ((StatefulRedisClusterConnection<String, String>) c).sync() : c -> ((StatefulRedisConnection<String, String>) c).sync()).dateFormat(new SimpleDateFormat(dateFormat)).variables(variables).fields(spel).build();
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
        if (redisearch.getPayloadField() == null) {
            new SuggestionItemProcessor<>(fieldExtractor(redisearch.getField()), scoreConverter());
        }
        return new PayloadSuggestionItemProcessor<>(fieldExtractor(redisearch.getField()), scoreConverter(), fieldExtractor(redisearch.getPayloadField()));
    }

    private ItemProcessor documentItemProcessor() {
        if (redisearch.getPayloadField() == null) {
            return new DocumentItemProcessor<>(keyMaker(), scoreConverter());
        }
        return new PayloadDocumentItemProcessor<>(keyMaker(), scoreConverter(), fieldExtractor(redisearch.getPayloadField()));
    }

    public ItemWriter<Object> writer() {
        switch (command) {
            case EVALSHA:
                return (ItemWriter) evalWriter();
            case EXPIRE:
                return (ItemWriter) expireWriter();
            case HMSET:
                return (ItemWriter) hashWriter();
            case GEOADD:
                return (ItemWriter) geoWriter();
            case LPUSH:
                return (ItemWriter) listWriter(false);
            case RPUSH:
                return (ItemWriter) listWriter(true);
            case SADD:
                return (ItemWriter) setWriter();
            case SET:
                return (ItemWriter) stringWriter();
            case NOOP:
                return (ItemWriter) noopWriter();
            case XADD:
                return (ItemWriter) streamWriter();
            case ZADD:
                return (ItemWriter) sortedSetWriter();
            case FTADD:
                return (ItemWriter) searchWriter();
            case FTSUGADD:
                return (ItemWriter) suggestWriter();
        }
        throw new IllegalArgumentException("Command not supported");
    }

    private RediSearchDocumentItemWriter<String, String> searchWriter() {
        AddOptions addOptions = AddOptions.builder().ifCondition(redisearch.getIfCondition()).language(redisearch.getLanguage()).noSave(redisearch.isNosave()).replace(redisearch.isReplace()).replacePartial(redisearch.isReplacePartial()).build();
        RediSearchDocumentItemWriter.Options options = RediSearchDocumentItemWriter.Options.builder().addOptions(addOptions).build();
        return RediSearchDocumentItemWriter.<String, String>builder().pool(rediSearchConnectionPool()).index(redisearch.getIndex()).options(options).build();
    }

    private RediSearchSuggestItemWriter<String, String> suggestWriter() {
        RediSearchSuggestItemWriter.Options options = RediSearchSuggestItemWriter.Options.builder().increment(redisearch.isIncrement()).build();
        return RediSearchSuggestItemWriter.<String, String>builder().pool(rediSearchConnectionPool()).key(redisearch.getIndex()).options(options).build();
    }

    private GenericObjectPool<StatefulRediSearchConnection<String, String>> rediSearchConnectionPool() {
        return connectionPool(rediSearchClient(getRedisOptions()));
    }

    private GenericObjectPool<StatefulRedisClusterConnection<String, String>> clusterConnectionPool() {
        return connectionPool(getRedisOptions().redisClusterClient());
    }

    private GenericObjectPool<StatefulRedisConnection<String, String>> connectionPool() {
        return connectionPool(getRedisOptions().redisClient());
    }

    private long commandTimeout() {
        return getRedisOptions().getCommandTimeout();
    }

    private boolean isCluster() {
        return getRedisOptions().isCluster();
    }


    private ItemWriter<Map<String, String>> evalWriter() {
        if (isCluster()) {
            return new RedisClusterDataStructureItemWriters.RedisClusterEvalItemWriter<>(clusterConnectionPool(), commandTimeout(), commandOptions.getEvalSha(), commandOptions.getEvalOutputType(), MapToArrayConverter.builder().fields(keyFields).build(), MapToArrayConverter.builder().fields(commandOptions.getEvalArgs()).build());
        }
        return new RedisDataStructureItemWriters.RedisEvalItemWriter<>(connectionPool(), commandTimeout(), commandOptions.getEvalSha(), commandOptions.getEvalOutputType(), MapToArrayConverter.builder().fields(keyFields).build(), MapToArrayConverter.builder().fields(commandOptions.getEvalArgs()).build());
    }

    private ItemWriter<Map<String, String>> noopWriter() {
        if (isCluster()) {
            return new RedisClusterDataStructureItemWriters.RedisClusterNoopItemWriter<>(clusterConnectionPool(), commandTimeout());
        }
        return new RedisDataStructureItemWriters.RedisNoopItemWriter<>(connectionPool(), commandTimeout());
    }

    private ItemWriter<Map<String, String>> streamWriter() {
        if (isCluster()) {
            return new RedisClusterDataStructureItemWriters.RedisClusterStreamItemWriter<>(clusterConnectionPool(), commandTimeout(), keyMaker(), new IdemConverter<>(), fieldExtractor(commandOptions.getIdField()), commandOptions.getMaxlen(), commandOptions.isApproximateTrimming());
        }
        return new RedisDataStructureItemWriters.RedisStreamItemWriter<>(connectionPool(), commandTimeout(), keyMaker(), new IdemConverter<>(), fieldExtractor(commandOptions.getIdField()), commandOptions.getMaxlen(), commandOptions.isApproximateTrimming());
    }

    private ItemWriter<Map<String, String>> sortedSetWriter() {
        if (isCluster()) {
            return new RedisClusterDataStructureItemWriters.RedisClusterSortedSetItemWriter<>(clusterConnectionPool(), commandTimeout(), keyMaker(), memberIdMaker(), scoreConverter());
        }
        return new RedisDataStructureItemWriters.RedisSortedSetItemWriter<>(connectionPool(), commandTimeout(), keyMaker(), memberIdMaker(), scoreConverter());
    }

    private ItemWriter<Map<String, String>> hashWriter() {
        if (isCluster()) {
            return new RedisClusterDataStructureItemWriters.RedisClusterHashItemWriter<>(clusterConnectionPool(), commandTimeout(), keyMaker(), new IdemConverter<>());
        }
        return new RedisDataStructureItemWriters.RedisHashItemWriter<>(connectionPool(), commandTimeout(), keyMaker(), new IdemConverter<>());
    }

    private ItemWriter<Map<String, String>> geoWriter() {
        if (isCluster()) {
            return new RedisClusterDataStructureItemWriters.RedisClusterGeoSetItemWriter<>(clusterConnectionPool(), commandTimeout(), keyMaker(), memberIdMaker(), doubleFieldExtractor(commandOptions.getLongitudeField()), doubleFieldExtractor(commandOptions.getLatitudeField()));
        }
        return new RedisDataStructureItemWriters.RedisGeoSetItemWriter<>(connectionPool(), commandTimeout(), keyMaker(), memberIdMaker(), doubleFieldExtractor(commandOptions.getLongitudeField()), doubleFieldExtractor(commandOptions.getLatitudeField()));
    }

    private ItemWriter<Map<String, String>> listWriter(boolean right) {
        if (isCluster()) {
            return new RedisClusterDataStructureItemWriters.RedisClusterListItemWriter<>(clusterConnectionPool(), commandTimeout(), keyMaker(), memberIdMaker(), right);
        }
        return new RedisDataStructureItemWriters.RedisListItemWriter<>(connectionPool(), commandTimeout(), keyMaker(), memberIdMaker(), right);
    }

    private ItemWriter<Map<String, String>> setWriter() {
        if (isCluster()) {
            return new RedisClusterDataStructureItemWriters.RedisClusterSetItemWriter<>(clusterConnectionPool(), commandTimeout(), keyMaker(), memberIdMaker());
        }
        return new RedisDataStructureItemWriters.RedisSetItemWriter<>(connectionPool(), commandTimeout(), keyMaker(), memberIdMaker());
    }

    private ItemWriter<Map<String, String>> stringWriter() {
        if (isCluster()) {
            return new RedisClusterDataStructureItemWriters.RedisClusterStringItemWriter<>(clusterConnectionPool(), commandTimeout(), keyMaker(), stringValueConverter());
        }
        return new RedisDataStructureItemWriters.RedisStringItemWriter<>(connectionPool(), commandTimeout(), keyMaker(), stringValueConverter());
    }

    private ItemWriter<Map<String, String>> expireWriter() {
        if (isCluster()) {
            return new RedisClusterDataStructureItemWriters.RedisClusterExpireItemWriter<>(clusterConnectionPool(), commandTimeout(), keyMaker(), longFieldExtractor(commandOptions.getTimeout()));
        }
        return new RedisDataStructureItemWriters.RedisExpireItemWriter<>(connectionPool(), commandTimeout(), keyMaker(), longFieldExtractor(commandOptions.getTimeout()));
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
