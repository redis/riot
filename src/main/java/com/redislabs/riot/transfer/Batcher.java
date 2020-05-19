package com.redislabs.riot.transfer;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

@Slf4j
public class Batcher<I, O> {

    private final ItemReader<I> reader;
    private final int batchSize;
    private final ItemProcessor<I, ? extends O> processor;
    private final ErrorHandler errorHandler;
    private final BlockingQueue<O> items;

    boolean finished = false;

    @Builder
    public Batcher(ItemReader<I> reader, int batchSize, ItemProcessor<I, ? extends O> processor, ErrorHandler errorHandler) {
        this.reader = reader;
        this.batchSize = batchSize;
        this.processor = processor;
        this.errorHandler = errorHandler;
        this.items = new LinkedBlockingDeque<>(batchSize);
    }

    public List<O> next() {
        if (finished) {
            return null;
        }
        while (items.size() < batchSize && !finished) {
            I item;
            try {
                item = reader.read();
            } catch (Exception e) {
                errorHandler.handle(e);
                continue;
            }
            if (item == null) {
                finished = true;
            } else {
                O processedItem;
                try {
                    processedItem = processor.process(item);
                } catch (Exception e) {
                    log.error("Could not process item", e);
                    continue;
                }
                if (processedItem != null) {
                    try {
                        items.put(processedItem);
                    } catch (InterruptedException e) {
                        return null;
                    }
                }
            }
        }
        return flush();
    }

    public List<O> flush() {
        List<O> result = new ArrayList<>(items.size());
        items.drainTo(result);
        return result;
    }

}
