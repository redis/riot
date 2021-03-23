package com.redislabs.riot;

import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.support.BoundedItemReader;
import org.springframework.batch.item.redis.support.JobFactory;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.SyncTaskExecutor;

import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

@Slf4j
@Setter
@Accessors(fluent = true)
public class StepBuilder<I, O> {

    private final JobFactory jobFactory;
    private final TransferOptions options;

    private String name;
    private String taskName;
    private ItemReader<I> reader;
    private ItemProcessor<I, O> processor;
    private ItemWriter<O> writer;
    private Supplier<String> extraMessage;

    public StepBuilder(JobFactory jobFactory, TransferOptions options) {
        this.jobFactory = jobFactory;
        this.options = options;
    }

    public SimpleStepBuilder<I, O> build() {
        if (options.getMaxItemCount() != null) {
            if (reader instanceof AbstractItemCountingItemStreamItemReader) {
                log.info("Setting max item count to {} on reader", options.getMaxItemCount());
                ((AbstractItemCountingItemStreamItemReader<I>) reader).setMaxItemCount(Math.toIntExact(options.getMaxItemCount()));
            }
        }
        SimpleStepBuilder<I, O> step = jobFactory.step(name).<I, O>chunk(options.getChunkSize()).reader(reader).processor(processor).writer(writer);
        if (options.isShowProgress()) {
            ProgressMonitor.ProgressMonitorBuilder<I, O> monitorBuilder = ProgressMonitor.<I, O>builder().taskName(taskName).max(max(reader));
            if (extraMessage != null) {
                monitorBuilder.extraMessageSupplier(extraMessage);
            }
            ProgressMonitor<I, O> monitor = monitorBuilder.build();
            step.listener((StepExecutionListener) monitor);
            step.listener((ItemWriteListener<? super O>) monitor);
        }
        FaultTolerantStepBuilder<I, O> ftStep = step.faultTolerant().skipLimit(options.getSkipLimit()).skip(ExecutionException.class);
        if (options.getThreads() > 1) {
            SimpleAsyncTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();
            taskExecutor.setConcurrencyLimit(options.getThreads());
            ftStep.taskExecutor(taskExecutor).throttleLimit(options.getThreads());
        } else {
            ftStep.taskExecutor(new SyncTaskExecutor());
        }
        return ftStep;
    }

    private Long max(ItemReader<?> reader) {
        if (reader instanceof BoundedItemReader) {
            return ((BoundedItemReader<?>) reader).size();
        }
        return null;
    }

}