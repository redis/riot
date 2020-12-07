package com.redislabs.riot;

import java.util.List;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.support.MultiTransferExecution;
import org.springframework.batch.item.redis.support.MultiTransferExecutionListenerAdapter;
import org.springframework.batch.item.redis.support.Transfer;
import org.springframework.batch.item.redis.support.TransferExecution;
import org.springframework.batch.item.redis.support.TransferOptions;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

import io.lettuce.core.AbstractRedisClient;
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
	@Option(names = "--no-progress", description = "Disable progress bars")
	private boolean disableProgress;

	@Override
	protected void execute(RedisOptions redisOptions) throws Exception {
		execution(redisOptions).start().get();
	}

	public MultiTransferExecution execution(RedisOptions redisOptions) throws Exception {
		AbstractRedisClient client = redisOptions.client();
		List<Transfer<I, O>> transfers = transfers(
				TransferContext.builder().client(client).redisOptions(redisOptions).build());
		MultiTransferExecution execution = new MultiTransferExecution(transfers);
		configure(execution);
		execution.addListener(new MultiTransferExecutionListenerAdapter() {
			@Override
			public void onStart(TransferExecution<?, ?> execution) {
				if (disableProgress) {
					return;
				}
				execution.addListener(new ProgressReporter(execution));
			}

			@Override
			public void onComplete() {
				client.shutdown();
				client.getResources().shutdown();
			}

		});
		return execution;
	}

	protected void configure(MultiTransferExecution execution) {
	}

	protected abstract List<Transfer<I, O>> transfers(TransferContext context) throws Exception;

	protected Transfer<I, O> transfer(ItemReader<I> reader, ItemProcessor<I, O> processor, ItemWriter<O> writer)
			throws Exception {
		if (maxItemCount != null) {
			if (reader instanceof AbstractItemCountingItemStreamItemReader) {
				log.debug("Configuring reader with maxItemCount={}", maxItemCount);
				((AbstractItemCountingItemStreamItemReader<I>) reader).setMaxItemCount(maxItemCount);
			}
		}
		String readerName = name(reader);
		String name = String.format(transferNameFormat(), readerName);
		return Transfer.<I, O>builder().name(name).reader(reader).processor(processor).writer(writer)
				.options(TransferOptions.builder().batch(batch).threads(threads).build()).build();
	}

	protected abstract String transferNameFormat();

	private String name(ItemReader<I> reader) {
		if (reader instanceof ItemStreamSupport) {
			// this is a hack to get the source name
			String name = ((ItemStreamSupport) reader).getExecutionContextKey("");
			return name.substring(0, name.length() - 1);
		}
		return ClassUtils.getShortName(reader.getClass());

	}

}
