package com.redislabs.riot.stream;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.support.Transfer.TransferBuilder;

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
	protected TransferBuilder<I, O> transfer(ItemReader<I> reader, ItemProcessor<I, O> processor, ItemWriter<O> writer)
			throws Exception {
		return super.transfer(reader, processor, writer).flushInterval(Duration.of(flushInterval, ChronoUnit.MILLIS));
	}

	@Override
	protected String transferNameFormat() {
		return "Streaming from %s";
	}

}
