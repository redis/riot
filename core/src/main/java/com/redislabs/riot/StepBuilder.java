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
import org.springframework.batch.item.redis.support.JobFactory;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
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
    private Supplier<Long> initialMax;

    public StepBuilder(JobFactory jobFactory, TransferOptions options) {
        this.jobFactory = jobFactory;
        this.options = options;
    }

    public SimpleStepBuilder<I, O> build() {
        SimpleStepBuilder<I, O> step = jobFactory.step(name).<I, O>chunk(options.getChunkSize()).reader(reader).processor(processor).writer(writer);
        if (options.isShowProgress()) {
            ProgressMonitor.ProgressMonitorBuilder<I, O> monitorBuilder = ProgressMonitor.builder();
            monitorBuilder.taskName(taskName);
            monitorBuilder.initialMax(initialMax);
            monitorBuilder.updateIntervalMillis(options.getProgressUpdateIntervalMillis());
            monitorBuilder.extraMessage(extraMessage);
            ProgressMonitor<I, O> monitor = monitorBuilder.build();
            step.listener((StepExecutionListener) monitor);
            step.listener((ItemWriteListener<? super O>) monitor);
        }
        FaultTolerantStepBuilder<I, O> ftStep = step.faultTolerant().skipLimit(options.getSkipLimit()).skip(ExecutionException.class);
        if (options.getThreads() > 1) {
            ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
            taskExecutor.setCorePoolSize(options.getThreads());
            taskExecutor.setMaxPoolSize(options.getThreads());
            taskExecutor.setQueueCapacity(options.getThreads());
            taskExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
            taskExecutor.afterPropertiesSet();
            log.info("Created pooled task executor of size {}", taskExecutor.getCorePoolSize());
            ftStep.taskExecutor(taskExecutor).throttleLimit(options.getThreads());
        } else {
            ftStep.taskExecutor(new SyncTaskExecutor());
        }
        return ftStep;
    }

}