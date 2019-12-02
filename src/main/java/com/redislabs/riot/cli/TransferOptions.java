package com.redislabs.riot.cli;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;

import com.redislabs.riot.batch.ThrottlingItemReader;
import com.redislabs.riot.batch.ThrottlingItemStreamReader;

import lombok.Data;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.Option;

@Slf4j
@Accessors(fluent = true)
public @Data class TransferOptions {

	@Option(names = "--threads", description = "Thread count (default: ${DEFAULT-VALUE})", paramLabel = "<count>")
	private int threads = 1;
	@Option(names = { "-b",
			"--batch" }, description = "Number of items in each batch (default: ${DEFAULT-VALUE})", paramLabel = "<size>")
	private int batchSize = 50;
	@Option(names = { "-m", "--max" }, description = "Max number of items to read", paramLabel = "<count>")
	private Integer count;
	@Option(names = "--sleep", description = "Sleep duration in millis between reads", paramLabel = "<ms>")
	private Long sleep;

	public <I> ItemReader<I> configure(ItemReader<I> reader) {
		if (count != null) {
			if (reader instanceof AbstractItemCountingItemStreamItemReader) {
				((AbstractItemCountingItemStreamItemReader<I>) reader).setMaxItemCount(count);
			} else {
				log.warn("Count is set for a source that does not support capping");
			}
		}
		if (sleep == null) {
			return reader;
		}
		if (reader instanceof ItemStreamReader) {
			return new ThrottlingItemStreamReader<I>((ItemStreamReader<I>) reader, sleep);
		}
		return new ThrottlingItemReader<I>(reader, sleep);
	}

}
