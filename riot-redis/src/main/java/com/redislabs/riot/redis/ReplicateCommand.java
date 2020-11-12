package com.redislabs.riot.redis;

import java.util.ArrayList;
import java.util.List;

import org.springframework.batch.item.redis.RedisDumpItemReader;
import org.springframework.batch.item.redis.RedisDumpItemReader.RedisDumpItemReaderBuilder;
import org.springframework.batch.item.redis.RedisDumpItemWriter;
import org.springframework.batch.item.redis.support.AbstractKeyValueItemReader.KeyValueItemReaderBuilder;
import org.springframework.batch.item.redis.support.KeyValue;
import org.springframework.batch.item.redis.support.LiveKeyItemReader;
import org.springframework.batch.item.redis.support.Transfer;

import com.redislabs.riot.AbstractTransferCommand;
import com.redislabs.riot.RedisConnectionOptions;
import com.redislabs.riot.RedisExportOptions;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

@Command(name = "replicate", aliases = {
	"r" }, description = "Replicate a source Redis database in a target Redis database")
public class ReplicateCommand extends AbstractTransferCommand<KeyValue<byte[]>, KeyValue<byte[]>> {

    @Mixin
    private RedisConnectionOptions targetRedis = new RedisConnectionOptions();
    @Mixin
    private RedisExportOptions options = new RedisExportOptions();
    @Option(names = "--live", description = "Enable live replication")
    private boolean live;
    @Option(names = "--notification-queue", description = "Capacity of the keyspace notification queue (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
    private int notificationQueueCapacity = KeyValueItemReaderBuilder.DEFAULT_NOTIFICATION_QUEUE_CAPACITY;
    @Option(names = "--flush-interval", description = "Duration between notification queue flushes (default: ${DEFAULT-VALUE})", paramLabel = "<ms>")
    private long flushPeriod = 50;

    @Override
    protected List<Transfer<KeyValue<byte[]>, KeyValue<byte[]>>> transfers() throws Exception {
	List<Transfer<KeyValue<byte[]>, KeyValue<byte[]>>> transfers = new ArrayList<>();
	RedisDumpItemWriter writer = configure(RedisDumpItemWriter.builder().replace(true), targetRedis).build();
	writer.setName(toString(targetRedis.redisURI()));
	RedisDumpItemReader reader = readerBuilder().scanCount(options.getScanCount()).build();
	reader.setName(String.format("Scanning %s", options.getScanMatch()));
	transfers.add(transfer(reader, null, writer));
	if (live) {
	    RedisDumpItemReader liveReader = readerBuilder().notificationQueueCapacity(notificationQueueCapacity)
		    .live(true).build();
	    liveReader.setName(String.format("Listening to %s",
		    ((LiveKeyItemReader) liveReader.getKeyReader()).getPubSubPattern()));
	    Transfer<KeyValue<byte[]>, KeyValue<byte[]>> transfer = transfer(liveReader, null, writer);
	    transfer.setFlushPeriod(flushPeriod);
	    transfers.add(transfer);
	}
	return transfers;
    }

    private RedisDumpItemReaderBuilder readerBuilder() throws Exception {
	return configure(
		RedisDumpItemReader.builder().batch(options.getReaderBatchSize()).threads(options.getReaderThreads())
			.queueCapacity(options.getQueueCapacity()).scanMatch(options.getScanMatch()));
    }

    @Override
    protected String transferNameFormat() {
	return "%s";
    }

}
