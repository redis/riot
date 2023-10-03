package com.redis.riot.cli;

import java.time.Instant;
import java.util.List;

import com.redis.riot.core.GeneratorImport;
import com.redis.riot.core.StepBuilder;
import com.redis.spring.batch.common.DataType;
import com.redis.spring.batch.common.Range;
import com.redis.spring.batch.gen.CollectionOptions;
import com.redis.spring.batch.gen.GeneratorItemReader;
import com.redis.spring.batch.gen.MapOptions;
import com.redis.spring.batch.gen.StreamOptions;
import com.redis.spring.batch.gen.StringOptions;
import com.redis.spring.batch.gen.TimeSeriesOptions;
import com.redis.spring.batch.gen.ZsetOptions;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "generate", description = "Generate data structures.")
public class GenerateCommand extends AbstractStructImportCommand {

    @Option(names = "--count", description = "Number of items to generate (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    int count = GeneratorImport.DEFAULT_COUNT;

    @Option(names = "--keyspace", description = "Keyspace prefix for generated data structures (default: ${DEFAULT-VALUE}).", paramLabel = "<str>")
    String keyspace = GeneratorItemReader.DEFAULT_KEYSPACE;

    @Option(names = "--keys", description = "Start and end index for keys (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
    Range keyRange = GeneratorItemReader.DEFAULT_KEY_RANGE;

    @Option(arity = "1..*", names = "--types", description = "Data structure types to generate: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<type>")
    List<DataType> types = GeneratorItemReader.defaultTypes();

    @Option(names = "--expiration", description = "TTL in seconds.", paramLabel = "<secs>")
    Range expiration;

    @Option(names = "--hash-fields", description = "Number of fields in hashes (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
    Range hashFieldCount = MapOptions.DEFAULT_FIELD_COUNT;

    @Option(names = "--hash-field-len", description = "Value size for hash fields (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
    Range hashFieldLength = MapOptions.DEFAULT_FIELD_LENGTH;

    @Option(names = "--json-fields", description = "Number of fields in JSON docs (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
    Range jsonFieldCount = MapOptions.DEFAULT_FIELD_COUNT;

    @Option(names = "--json-field-len", description = "Value size for JSON fields (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
    Range jsonFieldLength = MapOptions.DEFAULT_FIELD_LENGTH;

    @Option(names = "--list-members", description = "Number of elements in lists (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
    Range listMemberCount = CollectionOptions.DEFAULT_MEMBER_COUNT;

    @Option(names = "--list-member-len", description = "Value size for list elements (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
    Range listMemberRange = CollectionOptions.DEFAULT_MEMBER_RANGE;

    @Option(names = "--set-members", description = "Number of elements in sets (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
    Range setMemberCount = CollectionOptions.DEFAULT_MEMBER_COUNT;

    @Option(names = "--set-member-len", description = "Value size for set elements (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
    Range setMemberLength = CollectionOptions.DEFAULT_MEMBER_RANGE;

    @Option(names = "--stream-messages", description = "Number of messages in streams (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
    Range streamMessageCount = StreamOptions.DEFAULT_MESSAGE_COUNT;

    @Option(names = "--stream-fields", description = "Number of fields in stream messages (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
    Range streamFieldCount = MapOptions.DEFAULT_FIELD_COUNT;

    @Option(names = "--stream-field-len", description = "Value size for fields in stream messages (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
    Range streamFieldLength = MapOptions.DEFAULT_FIELD_LENGTH;

    @Option(names = "--string-len", description = "Length of strings (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
    Range stringLength = StringOptions.DEFAULT_LENGTH;

    @Option(names = "--ts-samples", description = "Number of samples in timeseries (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
    Range timeseriesSampleCount = TimeSeriesOptions.DEFAULT_SAMPLE_COUNT;

    @Option(names = "--ts-time", description = "Start time for samples in timeseries, e.g. 2007-12-03T10:15:30.00Z (default: now).", paramLabel = "<epoch>")
    Instant timeseriesStartTime;

    @Option(names = "--zset-members", description = "Number of elements in sorted sets (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
    Range zsetMemberCount = CollectionOptions.DEFAULT_MEMBER_COUNT;

    @Option(names = "--zset-member-len", description = "Value size for sorted-set elements (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
    Range zsetMemberLength = CollectionOptions.DEFAULT_MEMBER_RANGE;

    @Option(names = "--zset-score", description = "Score of sorted sets (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
    Range zsetScore = ZsetOptions.DEFAULT_SCORE;

    @Override
    protected String taskName(StepBuilder<?, ?> step) {
        return "Generating";
    }

    @Override
    protected long size(StepBuilder<?, ?> step) {
        return count;
    }

    @Override
    protected GeneratorImport getKeyValueImportExecutable() {
        GeneratorImport executable = new GeneratorImport();
        executable.setCount(count);
        executable.setExpiration(expiration);
        executable.setHashOptions(hashOptions());
        executable.setJsonOptions(jsonOptions());
        executable.setKeyRange(keyRange);
        executable.setKeyspace(keyspace);
        executable.setListOptions(listOptions());
        executable.setSetOptions(setOptions());
        executable.setStreamOptions(streamOptions());
        executable.setStringOptions(stringOptions());
        executable.setTimeSeriesOptions(timeseriesOptions());
        executable.setTypes(types);
        executable.setZsetOptions(zsetOptions());
        return executable;
    }

    private ZsetOptions zsetOptions() {
        ZsetOptions options = new ZsetOptions();
        options.setMemberCount(zsetMemberCount);
        options.setMemberRange(zsetMemberLength);
        options.setScore(zsetScore);
        return options;
    }

    private TimeSeriesOptions timeseriesOptions() {
        TimeSeriesOptions options = new TimeSeriesOptions();
        options.setSampleCount(timeseriesSampleCount);
        options.setStartTime(timeseriesStartTime);
        return options;
    }

    private StringOptions stringOptions() {
        StringOptions options = new StringOptions();
        options.setLength(stringLength);
        return options;
    }

    private StreamOptions streamOptions() {
        StreamOptions options = new StreamOptions();
        options.setBodyOptions(mapOptions(streamFieldCount, streamFieldLength));
        options.setMessageCount(streamMessageCount);
        return options;
    }

    private CollectionOptions setOptions() {
        return collectionOptions(setMemberCount, setMemberLength);
    }

    private CollectionOptions listOptions() {
        return collectionOptions(listMemberCount, listMemberRange);
    }

    private CollectionOptions collectionOptions(Range memberCount, Range memberRange) {
        CollectionOptions options = new CollectionOptions();
        options.setMemberCount(memberCount);
        options.setMemberRange(memberRange);
        return options;
    }

    private MapOptions jsonOptions() {
        return mapOptions(jsonFieldCount, jsonFieldLength);
    }

    private MapOptions hashOptions() {
        return mapOptions(hashFieldCount, hashFieldLength);
    }

    private MapOptions mapOptions(Range fieldCount, Range fieldLength) {
        MapOptions options = new MapOptions();
        options.setFieldCount(fieldCount);
        options.setFieldLength(fieldLength);
        return options;
    }

}
