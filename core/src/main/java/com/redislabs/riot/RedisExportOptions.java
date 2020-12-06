package com.redislabs.riot;

import org.springframework.batch.item.redis.support.KeyReaderOptions;
import org.springframework.batch.item.redis.support.QueueOptions;
import org.springframework.batch.item.redis.support.ReaderOptions;
import org.springframework.batch.item.redis.support.TransferOptions;

import picocli.CommandLine.Option;

public class RedisExportOptions {

	@Option(names = "--scan-count", description = "SCAN COUNT option (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private long scanCount = KeyReaderOptions.DEFAULT_SCAN_COUNT;
	@Option(names = "--scan-match", description = "SCAN MATCH pattern (default: ${DEFAULT-VALUE})", paramLabel = "<glob>")
	private String scanMatch = KeyReaderOptions.DEFAULT_SCAN_MATCH;
	@Option(names = "--reader-queue", description = "Capacity of the reader queue (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private int queueCapacity = QueueOptions.DEFAULT_CAPACITY;
	@Option(names = "--reader-threads", description = "Number of reader threads (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private int threads = TransferOptions.DEFAULT_THREAD_COUNT;
	@Option(names = "--reader-batch", description = "Number of reader values to process at once (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private int batchSize = TransferOptions.DEFAULT_BATCH_SIZE;

	public ReaderOptions readerOptions() {
		return ReaderOptions.builder().transferOptions(transferOptions()).queueOptions(queueOptions())
				.keyReaderOptions(keyReaderOptions()).build();
	}

	private QueueOptions queueOptions() {
		return QueueOptions.builder().capacity(queueCapacity).build();
	}

	private KeyReaderOptions keyReaderOptions() {
		return KeyReaderOptions.builder().queueOptions(queueOptions()).scanCount(scanCount).scanMatch(scanMatch)
				.build();
	}

	private TransferOptions transferOptions() {
		return TransferOptions.builder().threads(threads).batch(batchSize).build();
	}
}
