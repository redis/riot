package com.redislabs.riot;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;

import lombok.extern.slf4j.Slf4j;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;
import picocli.CommandLine.Option;

@Slf4j
public abstract class AbstractTransferCommand<I, O> extends RiotCommand {

    @Option(names = "--threads", description = "Thread count (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
    private int threads = 1;
    @Option(names = { "-b",
	    "--batch" }, description = "Number of items in each batch (default: ${DEFAULT-VALUE})", paramLabel = "<size>")
    private int batch = 50;
    @Option(names = "--max", description = "Max number of items to read", paramLabel = "<count>")
    private Integer maxItemCount;

    @Override
    public void run() {
	List<Transfer<I, O>> transfers;
	try {
	    transfers = transfers();
	} catch (Exception e) {
	    log.error("Could not create transfer", e);
	    return;
	}
	for (Transfer<I, O> transfer : transfers) {
	    open(transfer);
	    CompletableFuture<Void> future;
	    try {
		future = transfer.executeAsync();
	    } catch (Exception e) {
		log.error("Could not initialize transfer", e);
		continue;
	    }
	    ProgressBarBuilder builder = new ProgressBarBuilder();
	    Long total = transfer.getTotal();
	    if (total != null) {
		builder.setInitialMax(total);
	    }
	    builder.setTaskName(taskName() + " " + name(transfer.getReader()));
	    builder.showSpeed();
	    try (ProgressBar progressBar = builder.build()) {
		ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
		ScheduledFuture<?> progressFuture = scheduler.scheduleAtFixedRate(
			() -> progressBar.stepTo(transfer.getDone()), 0, 100, TimeUnit.MILLISECONDS);
		try {
		    future.get();
		} catch (InterruptedException e) {
		    // ignore
		    log.error("Interrupted", e);
		} catch (ExecutionException e) {
		    log.error("Error during transfer", e);
		} finally {
		    scheduler.shutdown();
		    progressFuture.cancel(true);
		    progressBar.stepTo(transfer.getDone());
		    close(transfer);
		}
	    }
	}
    }

    public void open(Transfer<I, O> transfer) {
	ExecutionContext executionContext = new ExecutionContext();
	if (transfer.getWriter() instanceof ItemStream) {
	    log.debug("Opening writer");
	    ((ItemStream) transfer.getWriter()).open(executionContext);
	}
	if (transfer.getReader() instanceof ItemStream) {
	    log.debug("Opening reader");
	    ((ItemStream) transfer.getReader()).open(executionContext);
	}
    }

    public void close(Transfer<I, O> transfer) {
	if (transfer.getReader() instanceof ItemStream) {
	    log.debug("Closing reader");
	    ((ItemStream) transfer.getReader()).close();
	}
	if (transfer.getWriter() instanceof ItemStream) {
	    log.debug("Closing writer");
	    ((ItemStream) transfer.getWriter()).close();
	}
    }

    public List<Transfer<I, O>> transfers() throws Exception {
	List<Transfer<I, O>> transfers = new ArrayList<>();
	for (ItemReader<I> reader : readers()) {
	    if (maxItemCount != null) {
		if (reader instanceof AbstractItemCountingItemStreamItemReader) {
		    log.debug("Configuring reader with maxItemCount={}", maxItemCount);
		    ((AbstractItemCountingItemStreamItemReader<I>) reader).setMaxItemCount(maxItemCount);
		}
	    }
	    transfers.add(transfer(reader, processor(), writer()));
	}
	return transfers;
    }

    protected abstract String taskName();

    protected abstract List<ItemReader<I>> readers() throws Exception;

    protected abstract ItemProcessor<I, O> processor() throws Exception;

    protected abstract ItemWriter<O> writer() throws Exception;

    protected Transfer<I, O> transfer(ItemReader<I> reader, ItemProcessor<I, O> processor, ItemWriter<O> writer) {
	return Transfer.<I, O>builder().reader(reader).processor(processor).writer(writer).batch(batch).threads(threads)
		.build();
    }

    private String name(ItemReader<I> reader) {
	if (reader instanceof ItemStreamSupport) {
	    // this is a hack to get the source name
	    String name = ((ItemStreamSupport) reader).getExecutionContextKey("");
	    return name.substring(0, name.length() - 1);
	}
	return "";
    }

}
