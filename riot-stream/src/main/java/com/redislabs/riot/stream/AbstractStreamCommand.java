package com.redislabs.riot.stream;

import java.util.List;

import com.redislabs.riot.AbstractTransferCommand;
import com.redislabs.riot.Transfer;

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
    private long flushPeriod = 50;

    @Override
    public List<Transfer<I, O>> transfers() {
	List<Transfer<I, O>> transfers = super.transfers();
	transfers.forEach(t -> t.setFlushPeriod(flushPeriod));
	return transfers;
    }

    @Override
    protected String transferNameFormat() {
	return "Streaming from %s";
    }

}
