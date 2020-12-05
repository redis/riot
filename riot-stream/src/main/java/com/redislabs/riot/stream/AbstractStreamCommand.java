package com.redislabs.riot.stream;

import java.time.Duration;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.support.Transfer;

import com.redislabs.riot.AbstractTransferCommand;

import lombok.Getter;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

@Command
public abstract class AbstractStreamCommand<I, O> extends AbstractTransferCommand<I, O> {

	@Mixin
	protected KafkaOptions kafkaOptions = new KafkaOptions();
	@Getter
	@Option(names = "--flush-interval", description = "Duration between batch flushes (default: ${DEFAULT-VALUE})", paramLabel = "<ms>")
	private long flushInterval = 50;

	@Override
	protected Transfer<I, O> transfer(ItemReader<I> reader, ItemProcessor<I, O> processor, ItemWriter<O> writer)
			throws Exception {
		Transfer<I, O> transfer = super.transfer(reader, processor, writer);
		transfer.getOptions().setFlushInterval(Duration.ofMillis(flushInterval));
		return transfer;
	}

	@Override
	protected String transferNameFormat() {
		return "Streaming from %s";
	}

}
