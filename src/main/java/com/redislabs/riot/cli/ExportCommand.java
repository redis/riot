package com.redislabs.riot.cli;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.RedisClusterKeyItemReader;
import org.springframework.batch.item.redis.RedisClusterKeyValueItemReader;
import org.springframework.batch.item.redis.RedisKeyItemReader;
import org.springframework.batch.item.redis.RedisKeyValueItemReader;
import org.springframework.batch.item.redis.support.ReaderOptions;
import org.springframework.batch.item.redis.support.TypeKeyValue;
import picocli.CommandLine;

@CommandLine.Command(name = "export", description = "Export data from Redis", subcommands = {FileExportCommand.class, DatabaseExportCommand.class})
public class ExportCommand extends TransferCommand {

    @CommandLine.Mixin
    private ExportOptions exportOptions = new ExportOptions();
    @CommandLine.Option(names = "--timeout", description = "Command timeout duration in seconds (default: ${DEFAULT-VALUE})", paramLabel = "<sec>")
    private long timeout = RedisURI.DEFAULT_TIMEOUT;
    @CommandLine.Option(names = "--key-regex", description = "Regular expression for key-field extraction", paramLabel = "<regex>")
    private String keyRegex;

    public ItemReader<TypeKeyValue<String>> reader() {
        ReaderOptions readerOptions = ReaderOptions.builder().batchSize(exportOptions.getBatchSize()).commandTimeout(getRedisOptions().getCommandTimeout()).queueCapacity(exportOptions.getQueueCapacity()).threadCount(exportOptions.getThreads()).build();
        if (getRedisOptions().isCluster()) {
            RedisClusterClient redisClusterClient = getRedisOptions().redisClusterClient();
            RedisClusterKeyItemReader<String, String> keyReader = RedisClusterKeyItemReader.builder().redisClusterClient(redisClusterClient).scanCount(exportOptions.getScanCount()).scanPattern(exportOptions.getScanMatch()).build();
            return RedisClusterKeyValueItemReader.<String, String>builder().pool(connectionPool(redisClusterClient)).keyReader(keyReader).options(readerOptions).build();
        }
        RedisClient redisClient = getRedisOptions().redisClient();
        RedisKeyItemReader<String, String> keyReader = RedisKeyItemReader.builder().redisClient(redisClient).scanCount(exportOptions.getScanCount()).scanPattern(exportOptions.getScanMatch()).build();
        return RedisKeyValueItemReader.<String, String>builder().pool(connectionPool(redisClient)).keyReader(keyReader).options(readerOptions).build();
    }

}
