package com.redis.riot.cli;

import com.redis.riot.core.GeneratorImport;
import com.redis.riot.core.StepBuilder;
import com.redis.spring.batch.gen.CollectionOptions;
import com.redis.spring.batch.gen.MapOptions;
import com.redis.spring.batch.gen.StreamOptions;
import com.redis.spring.batch.gen.StringOptions;
import com.redis.spring.batch.gen.TimeSeriesOptions;
import com.redis.spring.batch.gen.ZsetOptions;
import com.redis.spring.batch.util.LongRange;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "generate", description = "Generate data structures.")
public class GeneratorImportCommand extends AbstractKeyValueImportCommand {

    @ArgGroup(exclusive = false)
    GeneratorImportArgs args = new GeneratorImportArgs();

    @Override
    protected String taskName(StepBuilder<?, ?> step) {
        return "Generating";
    }

    @Override
    protected long size(StepBuilder<?, ?> step) {
        return args.count;
    }

    @Override
    protected GeneratorImport getKeyValueImportExecutable() {
        GeneratorImport executable = new GeneratorImport(redisClient());
        executable.setCount(args.count);
        executable.setExpiration(args.expiration);
        executable.setHashOptions(hashOptions());
        executable.setJsonOptions(jsonOptions());
        executable.setKeyRange(args.keyRange);
        executable.setKeyspace(args.keyspace);
        executable.setListOptions(listOptions());
        executable.setSetOptions(setOptions());
        executable.setStreamOptions(streamOptions());
        executable.setStringOptions(stringOptions());
        executable.setTimeSeriesOptions(timeseriesOptions());
        executable.setTypes(args.types);
        executable.setZsetOptions(zsetOptions());
        return executable;
    }

    private ZsetOptions zsetOptions() {
        ZsetOptions options = new ZsetOptions();
        options.setMemberCount(args.zsetMemberCount);
        options.setMemberRange(args.zsetMemberLength);
        options.setScore(args.zsetScore);
        return options;
    }

    private TimeSeriesOptions timeseriesOptions() {
        TimeSeriesOptions options = new TimeSeriesOptions();
        options.setSampleCount(args.timeseriesSampleCount);
        options.setStartTime(args.timeseriesStartTime);
        return options;
    }

    private StringOptions stringOptions() {
        StringOptions options = new StringOptions();
        options.setLength(args.stringLength);
        return options;
    }

    private StreamOptions streamOptions() {
        StreamOptions options = new StreamOptions();
        options.setBodyOptions(mapOptions(args.streamFieldCount, args.streamFieldLength));
        options.setMessageCount(args.streamMessageCount);
        return options;
    }

    private CollectionOptions setOptions() {
        return collectionOptions(args.setMemberCount, args.setMemberLength);
    }

    private CollectionOptions listOptions() {
        return collectionOptions(args.listMemberCount, args.listMemberRange);
    }

    private CollectionOptions collectionOptions(LongRange memberCount, LongRange memberRange) {
        CollectionOptions options = new CollectionOptions();
        options.setMemberCount(memberCount);
        options.setMemberRange(memberRange);
        return options;
    }

    private MapOptions jsonOptions() {
        return mapOptions(args.jsonFieldCount, args.jsonFieldLength);
    }

    private MapOptions hashOptions() {
        return mapOptions(args.hashFieldCount, args.hashFieldLength);
    }

    private MapOptions mapOptions(LongRange fieldCount, LongRange fieldLength) {
        MapOptions options = new MapOptions();
        options.setFieldCount(fieldCount);
        options.setFieldLength(fieldLength);
        return options;
    }

}
