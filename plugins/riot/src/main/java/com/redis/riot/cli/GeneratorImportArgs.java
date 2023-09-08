package com.redis.riot.cli;

import java.time.Instant;
import java.util.List;

import com.redis.riot.core.GeneratorImport;
import com.redis.spring.batch.gen.CollectionOptions;
import com.redis.spring.batch.gen.DataType;
import com.redis.spring.batch.gen.GeneratorItemReader;
import com.redis.spring.batch.gen.MapOptions;
import com.redis.spring.batch.gen.StreamOptions;
import com.redis.spring.batch.gen.StringOptions;
import com.redis.spring.batch.gen.TimeSeriesOptions;
import com.redis.spring.batch.gen.ZsetOptions;
import com.redis.spring.batch.util.DoubleRange;
import com.redis.spring.batch.util.LongRange;

import picocli.CommandLine.Option;

public class GeneratorImportArgs {

    @Option(names = "--count", description = "Number of items to generate (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    int count = GeneratorImport.DEFAULT_COUNT;

    @Option(names = "--keyspace", description = "Keyspace prefix for generated data structures (default: ${DEFAULT-VALUE}).", paramLabel = "<str>")
    String keyspace = GeneratorItemReader.DEFAULT_KEYSPACE;

    @Option(names = "--keys", description = "Start and end index for keys (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
    LongRange keyRange = GeneratorItemReader.DEFAULT_KEY_RANGE;

    @Option(arity = "1..*", names = "--types", description = "Data structure types to generate: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<type>")
    List<DataType> types = GeneratorItemReader.defaultTypes();

    @Option(names = "--expiration", description = "TTL in seconds.", paramLabel = "<secs>")
    LongRange expiration;

    @Option(names = "--hash-fields", description = "Number of fields in hashes (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
    LongRange hashFieldCount = MapOptions.DEFAULT_FIELD_COUNT;

    @Option(names = "--hash-field-len", description = "Value size for hash fields (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
    LongRange hashFieldLength = MapOptions.DEFAULT_FIELD_LENGTH;

    @Option(names = "--json-fields", description = "Number of fields in JSON docs (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
    LongRange jsonFieldCount = MapOptions.DEFAULT_FIELD_COUNT;

    @Option(names = "--json-field-len", description = "Value size for JSON fields (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
    LongRange jsonFieldLength = MapOptions.DEFAULT_FIELD_LENGTH;

    @Option(names = "--list-members", description = "Number of elements in lists (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
    LongRange listMemberCount = CollectionOptions.DEFAULT_MEMBER_COUNT;

    @Option(names = "--list-member-len", description = "Value size for list elements (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
    LongRange listMemberRange = CollectionOptions.DEFAULT_MEMBER_RANGE;

    @Option(names = "--set-members", description = "Number of elements in sets (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
    LongRange setMemberCount = CollectionOptions.DEFAULT_MEMBER_COUNT;

    @Option(names = "--set-member-len", description = "Value size for set elements (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
    LongRange setMemberLength = CollectionOptions.DEFAULT_MEMBER_RANGE;

    @Option(names = "--stream-messages", description = "Number of messages in streams (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
    LongRange streamMessageCount = StreamOptions.DEFAULT_MESSAGE_COUNT;

    @Option(names = "--stream-fields", description = "Number of fields in stream messages (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
    LongRange streamFieldCount = MapOptions.DEFAULT_FIELD_COUNT;

    @Option(names = "--stream-field-len", description = "Value size for fields in stream messages (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
    LongRange streamFieldLength = MapOptions.DEFAULT_FIELD_LENGTH;

    @Option(names = "--string-len", description = "Length of strings (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
    LongRange stringLength = StringOptions.DEFAULT_LENGTH;

    @Option(names = "--ts-samples", description = "Number of samples in timeseries (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
    LongRange timeseriesSampleCount = TimeSeriesOptions.DEFAULT_SAMPLE_COUNT;

    @Option(names = "--ts-time", description = "Start time for samples in timeseries, e.g. 2007-12-03T10:15:30.00Z (default: now).", paramLabel = "<epoch>")
    Instant timeseriesStartTime;

    @Option(names = "--zset-members", description = "Number of elements in sorted sets (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
    LongRange zsetMemberCount = CollectionOptions.DEFAULT_MEMBER_COUNT;

    @Option(names = "--zset-member-len", description = "Value size for sorted-set elements (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
    LongRange zsetMemberLength = CollectionOptions.DEFAULT_MEMBER_RANGE;

    @Option(names = "--zset-score", description = "Score of sorted sets (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
    DoubleRange zsetScore = ZsetOptions.DEFAULT_SCORE;

}
