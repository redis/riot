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
public class Flow<I, O> {
    @Getter
    private final String name;
    @Getter
    private final ItemReader<I> reader;
    private final ItemProcessor<I, O> processor;
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

    public FlowExecution<I, O> execute() {
        List<FlowThread<I, O>> flowThreads = new ArrayList<>(nThreads);
        for (int index = 0; index < nThreads; index++) {
            Batcher<I, O> batcher = Batcher.<I, O>builder().reader(reader).processor(processor).batchSize(batchSize)
                    .errorHandler(errorHandler).build();
            flowThreads.add(FlowThread.<I, O>builder().flow(this).threadId(index).threads(nThreads).batcher(batcher)
                    .flushRate(flushRate).build());
        }
        return FlowExecution.<I, O>builder().threads(flowThreads).build().execute();
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
        listeners.forEach(l -> l.onClose(this));
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
        listeners.forEach(l -> l.onOpen(this));
    }

    public interface Listener<I,O> {

        void onOpen(Flow<I,O> flow);

        void onClose(Flow<I,O> flow);
    }
}
