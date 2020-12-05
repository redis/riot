package com.redislabs.riot.redis;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.KeyDumpItemReader;
import org.springframework.batch.item.redis.KeyDumpItemWriter;
import org.springframework.batch.item.redis.LiveKeyDumpItemReader;
import org.springframework.batch.item.redis.support.KeyValue;
import org.springframework.batch.item.redis.support.LiveKeyReaderOptions;
import org.springframework.batch.item.redis.support.LiveReaderOptions;
import org.springframework.batch.item.redis.support.MultiTransferExecution;
import org.springframework.batch.item.redis.support.MultiTransferExecutionListenerAdapter;
import org.springframework.batch.item.redis.support.QueueOptions;
import org.springframework.batch.item.redis.support.ReaderOptions;
import org.springframework.batch.item.redis.support.Transfer;
import org.springframework.batch.item.redis.support.TransferOptions;

import com.redislabs.riot.AbstractTransferCommand;
import com.redislabs.riot.RedisExportOptions;
import com.redislabs.riot.RedisOptions;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulConnection;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

@Command(name = "replicate", aliases = {
		"r" }, description = "Replicate a source Redis database in a target Redis database")
public class ReplicateCommand extends AbstractTransferCommand<KeyValue<byte[]>, KeyValue<byte[]>> {

	@Mixin
	private RedisOptions targetRedis = new RedisOptions();
	@Mixin
	private RedisExportOptions options = new RedisExportOptions();
	@Option(names = "--live", description = "Enable live replication")
	private boolean live;
	@Option(names = "--notif-queue", description = "Capacity of the keyspace notification queue (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private int notificationQueueCapacity = QueueOptions.DEFAULT_CAPACITY;
	@Option(names = "--flush-interval", description = "Duration between notification queue flushes (default: ${DEFAULT-VALUE})", paramLabel = "<ms>")
	private long flushInterval = 50;

	@Override
	protected List<Transfer<KeyValue<byte[]>, KeyValue<byte[]>>> transfers(RedisURI uri, AbstractRedisClient client,
			GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig) throws Exception {
		List<Transfer<KeyValue<byte[]>, KeyValue<byte[]>>> transfers = new ArrayList<>();
		AbstractRedisClient targetClient = targetRedis.client();
		ReaderOptions readerOptions = options.readerOptions();
		KeyDumpItemReader reader = KeyDumpItemReader.builder().options(readerOptions).client(client)
				.poolConfig(poolConfig).build();
		reader.setName(String.format("Scanning %s", readerOptions.getKeyReaderOptions().getScanMatch()));
		transfers.add(transfer(reader, null, writer(targetClient)));
		if (live) {
			TransferOptions readerTransferOptions = readerOptions.getTransferOptions();
			TransferOptions transferOptions = TransferOptions.builder().batch(readerTransferOptions.getBatch())
					.threads(readerTransferOptions.getThreads()).build();
			LiveKeyReaderOptions liveKeyReaderOptions = LiveKeyReaderOptions.builder().database(uri.getDatabase())
					.keyPattern(readerOptions.getKeyReaderOptions().getScanMatch())
					.queueOptions(QueueOptions.builder().capacity(notificationQueueCapacity).build()).build();
			LiveReaderOptions liveReaderOptions = LiveReaderOptions.builder()
					.queueOptions(readerOptions.getQueueOptions()).transferOptions(transferOptions)
					.liveKeyReaderOptions(liveKeyReaderOptions).build();
			LiveKeyDumpItemReader liveReader = LiveKeyDumpItemReader.builder().client(client).poolConfig(poolConfig)
					.options(liveReaderOptions).build();
			liveReader.setName("Listening to keyspace");
			Transfer<KeyValue<byte[]>, KeyValue<byte[]>> liveTransfer = transfer(liveReader, null,
					writer(targetClient));
			liveTransfer.getOptions().setFlushInterval(Duration.ofMillis(flushInterval));
			transfers.add(liveTransfer);
		}
		return transfers;
	}

	@Override
	protected void configure(MultiTransferExecution execution) {
		KeyDumpItemWriter writer = (KeyDumpItemWriter) execution.getExecutions().get(0).getTransfer().getWriter();
		execution.addListener(new MultiTransferExecutionListenerAdapter() {
			@Override
			public void onComplete() {
				AbstractRedisClient client = writer.getClient();
				client.shutdown();
				client.getResources().shutdown();
			}
		});
	}

	private ItemWriter<KeyValue<byte[]>> writer(AbstractRedisClient targetClient) {
		KeyDumpItemWriter writer = KeyDumpItemWriter.builder().client(targetClient).poolConfig(targetRedis.poolConfig())
				.replace(true).build();
		writer.setName(toString(targetRedis.redisURI()));
		return writer;
	}

	@Override
	protected String transferNameFormat() {
		return "%s";
	}

}
