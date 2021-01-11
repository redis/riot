package com.redislabs.riot;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.step.builder.AbstractTaskletStepBuilder;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.util.ClassUtils;
import picocli.CommandLine;

import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

@Slf4j
public abstract class AbstractTransferCommand<I, O> extends AbstractTaskCommand {

    @CommandLine.Option(names = "--no-progress", description = "Show progress bars. True by default.", negatable = true)
    private boolean showProgress = true;
    @CommandLine.Option(names = "--threads", description = "Thread count (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    private int threads = 1;
    @CommandLine.Option(names = {"-b", "--batch"}, description = "Number of items in each batch (default: ${DEFAULT-VALUE}).", paramLabel = "<size>")
    private int chunkSize = 50;
    @CommandLine.Option(names = "--max", description = "Max number of items to read.", paramLabel = "<count>")
    private Long maxItemCount;
    @CommandLine.Option(names = "--skip-limit", description = "Max number of failed items to skip before the transfer fails (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    private int skipLimit = 0;

    protected <S, T> SimpleStepBuilder<S, T> simpleStep(String name) {
        return step(name).chunk(chunkSize);
    }

    protected final <S, T> AbstractTaskletStepBuilder<SimpleStepBuilder<S, T>> step(String name, ItemReader<S> reader, ItemProcessor<S, T> processor, ItemWriter<T> writer, Supplier<String> extraMessageSupplier) throws Exception {
        return step(simpleStep(name), name, reader, processor, writer, extraMessageSupplier);
    }

    protected final <S, T> AbstractTaskletStepBuilder<SimpleStepBuilder<S, T>> step(String name, ItemReader<S> reader, ItemProcessor<S, T> processor, ItemWriter<T> writer) throws Exception {
        return step(name, reader, processor, writer, null);
    }

    private final <S, T> AbstractTaskletStepBuilder<SimpleStepBuilder<S, T>> step(SimpleStepBuilder<S, T> step, String name, ItemReader<S> reader, ItemProcessor<S, T> processor, ItemWriter<T> writer, Supplier<String> extraMessageSupplier) throws Exception {
        if (maxItemCount != null) {
            if (reader instanceof AbstractItemCountingItemStreamItemReader) {
                log.debug("Configuring reader with maxItemCount={}", maxItemCount);
                ((AbstractItemCountingItemStreamItemReader<I>) reader).setMaxItemCount(Math.toIntExact(maxItemCount));
            }
        }
        step.reader(reader).processor(processor).writer(writer);
        if (showProgress) {
            Long size = size();
            ProgressMonitor.ProgressMonitorBuilder<I, O> monitorBuilder = ProgressMonitor.<I, O>builder().taskName(name).max(size == null ? maxItemCount : size);
            if (extraMessageSupplier != null) {
                monitorBuilder.extraMessageSupplier(extraMessageSupplier);
            }
            ProgressMonitor<I, O> monitor = monitorBuilder.build();
            step.listener((StepExecutionListener) monitor);
            step.listener((ItemWriteListener<? super O>) monitor);
        }
        FaultTolerantStepBuilder<S, T> ftStep = step.faultTolerant().skipLimit(skipLimit).skip(ExecutionException.class);
        if (threads > 1) {
            ftStep.taskExecutor(taskExecutor()).throttleLimit(threads);
        }
        return ftStep;
    }

    private TaskExecutor taskExecutor() {
        SimpleAsyncTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();
        taskExecutor.setConcurrencyLimit(threads);
        return taskExecutor;
    }

    protected Long size() throws Exception {
        return null;
    }

    protected String name(ItemReader<I> reader) {
        if (reader instanceof ItemStreamSupport) {
            // this is a hack to get the source name
            String name = ((ItemStreamSupport) reader).getExecutionContextKey("");
            return name.substring(0, name.length() - 1);
        }
        return ClassUtils.getShortName(reader.getClass());
    }

}
