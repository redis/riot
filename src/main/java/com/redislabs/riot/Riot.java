package com.redislabs.riot;

import com.redislabs.lettuce.helper.PoolOptions;
import com.redislabs.lettuce.helper.RedisOptions;
import com.redislabs.picocliredis.Application;
import com.redislabs.picocliredis.RedisCommandLineOptions;
import com.redislabs.riot.cli.ExportCommand;
import com.redislabs.riot.cli.ImportCommand;
import com.redislabs.riot.cli.ReplicateCommand;
import com.redislabs.riot.cli.TestCommand;
import com.redislabs.riot.cli.file.RangeConverter;
import org.springframework.batch.item.file.transform.Range;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "riot", subcommands = {ImportCommand.class, ExportCommand.class, ReplicateCommand.class, TestCommand.class})
public class Riot extends Application {

    @CommandLine.Mixin
    private RedisCommandLineOptions redisCommandLineOptions = RedisCommandLineOptions.builder().build();

    public static void main(String[] args) {
        System.exit(new Riot().execute(args));
    }

    @Override
    protected void registerConverters(CommandLine commandLine) {
        commandLine.registerConverter(Range.class, new RangeConverter());
        super.registerConverters(commandLine);
    }

    public RedisOptions redisOptions() {
        return redisOptions(redisCommandLineOptions);
    }

    public static RedisOptions redisOptions(RedisCommandLineOptions options) {
        RedisOptions.RedisOptionsBuilder builder = RedisOptions.builder().redisURI(options.getRedisURI()).clientOptions(options.clientOptions()).cluster(options.isCluster());
        if (options.isShowMetrics()) {
            builder.clientResources(options.clientResources());
        }
        return builder.build();
    }


}
