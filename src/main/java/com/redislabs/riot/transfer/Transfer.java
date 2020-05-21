package com.redislabs.riot.transfer;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.*;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class Transfer<I, O> {

    @Getter
    private final ItemReader<I> reader;
    @Getter
    private final ItemProcessor<I, ? extends O> processor;
    @Getter
    private final ItemWriter<O> writer;
    private List<Listener> listeners = new ArrayList<>();
    @Getter
    private boolean open;

    public Transfer(ItemReader<I> reader, ItemProcessor<I, ? extends O> processor, ItemWriter<O> writer) {
        this.reader = reader;
        this.processor = processor;
        this.writer = writer;
    }

    public void addListener(Listener listener) {
        listeners.add(listener);
    }

    public TransferExecution<I, O> execute(TransferExecution.Options options) {
        return new TransferExecution<>(this, options).execute();
    }

    public void open(ExecutionContext executionContext) throws ItemStreamException {
        if (reader instanceof ItemStream) {
            log.debug("Opening reader");
            ((ItemStream) reader).open(executionContext);
        }
        if (writer instanceof ItemStream) {
            log.debug("Opening writer");
            ((ItemStream) writer).open(executionContext);
        }
        this.open = true;
        listeners.forEach(Listener::onOpen);
    }

    public void close() throws ItemStreamException {
        if (reader instanceof ItemStream) {
            ((ItemStream) reader).close();
        }
        log.debug("Closing writer");
        if (writer instanceof ItemStream) {
            ((ItemStream) writer).close();
        }
        this.open = false;
        listeners.forEach(Listener::onClose);
    }

    public void write(List<O> items) throws Exception {
        writer.write(items);
    }

    public interface Listener {

        void onOpen();

        void onClose();
    }

}
