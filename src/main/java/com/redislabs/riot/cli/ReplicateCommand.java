package com.redislabs.riot.cli;

import com.redislabs.picocliredis.RedisCommandLineOptions;
import com.redislabs.riot.transfer.Transfer;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScanArgs;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.RedisItemReader;
import org.springframework.batch.item.redis.RedisItemWriter;
import org.springframework.batch.item.redis.RedisKeyDumpItemReader;
import org.springframework.batch.item.redis.support.KeyDump;
import org.springframework.batch.item.redis.support.commands.Restore;
import org.springframework.batch.item.support.PassThroughItemProcessor;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;

@Command(name = "replicate", description = "Replicate a Redis database to another Redis database", sortOptions = false)
public class ReplicateCommand extends TransferCommand<KeyDump<String>, KeyDump<String>> {

    private final static String DATABASE_TOKEN = "{database}";

    @CommandLine.Mixin
    private RedisCommandLineOptions target = new RedisCommandLineOptions();
    @Option(names = "--event-queue", description = "Event queue capacity (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
    private int notificationQueue = 10000;
    @Option(names = "--event-channel", description = "Event pub/sub channel (default: ${DEFAULT-VALUE}). Blank to disable", paramLabel = "<str>")
    private String channel = "__keyspace@" + DATABASE_TOKEN + "__:*";
    @Option(names = "--no-replace", description = "No REPLACE modifier with RESTORE command")
    private boolean noReplace;
    @Option(names = "--flush-rate", description = "Duration between notification flushes (default: ${DEFAULT-VALUE})", paramLabel = "<ms>")
    private long flushRate = 50;
    @Option(names = "--syncer-timeout", description = "Syncer timeout duration (default: ${DEFAULT-VALUE})", paramLabel = "<sec>")
    private long timeout = RedisURI.DEFAULT_TIMEOUT;
    @Option(names = "--syncer-batch", description = "Number of values in dump pipeline (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
    private int syncerBatchSize = 50;
    @Option(names = "--syncer-queue", description = "Capacity of the value queue (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
    private int syncerQueueSize = 10000;
    @Option(names = "--syncer-threads", description = "Number of value reader threads (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
    private int syncerThreads = 1;
    @CommandLine.Mixin
    private ExportOptions exportOptions = new ExportOptions();
    @Option(names = "--mode", description = "Replication mode ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})")
    private RedisItemReader.RedisItemReaderBuilder.Mode readerMode = RedisItemReader.RedisItemReaderBuilder.Mode.SCAN;

    @Override
    protected Transfer<KeyDump<String>, KeyDump<String>> getTransfer() throws Exception {
        ScanArgs scanArgs = ScanArgs.Builder.limit(exportOptions.getScanCount()).match(exportOptions.getScanMatch());
        RedisItemReader.Options readerOptions = RedisItemReader.Options.builder().batchSize(exportOptions.getBatchSize()).threads(exportOptions.getThreads()).queueCapacity(exportOptions.getQueueCapacity()).build();
        RedisKeyDumpItemReader<String> reader = RedisKeyDumpItemReader.builder().redisOptions(redisOptions()).mode(readerMode).options(readerOptions).scanArgs(scanArgs).build();
        RedisItemWriter<String, String, KeyDump<String>> writer = RedisItemWriter.<KeyDump<String>>builder().writeCommand(new Restore<>()).redisOptions(redisOptions(target)).build();
        return new Transfer<>(reader, new PassThroughItemProcessor<>(), writer);
    }

    @Override
    protected String taskName() {
        return "Replicating";
    }


}
