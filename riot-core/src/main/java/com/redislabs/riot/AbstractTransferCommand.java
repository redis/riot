package com.redislabs.riot;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;

import lombok.extern.slf4j.Slf4j;
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
	transfers.forEach(this::open);
	executeAsync(transfers).join();
	transfers.forEach(this::close);
    }

    public CompletableFuture<Void> executeAsync(List<Transfer<I, O>> transfers) {
	List<CompletableFuture<Void>> futures = new ArrayList<>();
	for (Transfer<I, O> transfer : transfers) {
	    CompletableFuture<Void> transferFuture = transfer.executeAsync();
	    String taskName = taskName() + " " + name(transfer.getReader());
	    TransferProgressMonitor progressMonitor = new TransferProgressMonitor(transfer, taskName);
	    transferFuture.whenComplete((k, t) -> progressMonitor.stop());
	    futures.add(CompletableFuture.allOf(transferFuture, CompletableFuture.runAsync(progressMonitor)));
	}
	return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
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
	    transfers.add(Transfer.<I, O>builder().reader(reader).processor(processor()).writer(writer()).batch(batch)
		    .threads(threads).build());
	}
	return transfers;
    }

    protected abstract String taskName();

    protected abstract List<ItemReader<I>> readers() throws Exception;

    protected abstract ItemProcessor<I, O> processor() throws Exception;

    protected abstract ItemWriter<O> writer() throws Exception;

    private String name(ItemReader<I> reader) {
	if (reader instanceof ItemStreamSupport) {
	    // this is a hack to get the source name
	    String name = ((ItemStreamSupport) reader).getExecutionContextKey("");
	    return name.substring(0, name.length() - 1);
	}
	return "";
    }

}
