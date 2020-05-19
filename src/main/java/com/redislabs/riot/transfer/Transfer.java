package com.redislabs.riot.transfer;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.*;

import java.util.ArrayList;
import java.util.List;

@Builder
@Slf4j
public class Transfer<I, O> {
    @Getter
    private final ItemReader<I> reader;
    private final ItemProcessor<I, ? extends O> processor;
    @Getter
    private final ItemWriter<O> writer;
    private final int nThreads;
    private final int batchSize;
    @Setter
    private Long flushRate;
    private final ErrorHandler errorHandler;
    private final List<Listener> listeners = new ArrayList<>();
    @Getter
    private boolean open;

    public void addListener(Listener listener) {
        listeners.add(listener);
    }

    public TransferExecution<I, O> execute() {
        List<TransferExecutor<I, O>> transferExecutors = new ArrayList<>(nThreads);
        for (int index = 0; index < nThreads; index++) {
            Batcher<I, O> batcher = Batcher.<I, O>builder().reader(reader).processor(processor).batchSize(batchSize).errorHandler(errorHandler).build();
            transferExecutors.add(TransferExecutor.<I, O>builder().transfer(this).id(index).threads(nThreads).batcher(batcher).flushRate(flushRate).build());
        }
        return TransferExecution.<I, O>builder().threads(transferExecutors).build().execute();
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

    public interface Listener {

        void onOpen();

        void onClose();
    }
}
