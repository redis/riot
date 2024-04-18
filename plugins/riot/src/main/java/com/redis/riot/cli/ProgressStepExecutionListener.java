package com.redis.riot.cli;

import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.listener.ItemListenerSupport;
import org.springframework.batch.item.Chunk;

import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;

/**
 * Listener tracking writer or step progress with by a progress bar.
 * 
 * @author Julien Ruaux
 * @since 3.1.2
 */
@SuppressWarnings("rawtypes")
public class ProgressStepExecutionListener extends ItemListenerSupport implements StepExecutionListener {

	public static final long UNKNOWN_SIZE = -1;
	public static final String EMPTY_STRING = "";

	private final Logger log = LoggerFactory.getLogger(ProgressStepExecutionListener.class);

	private final ProgressBarBuilder builder;

	private LongSupplier initialMaxSupplier = () -> UNKNOWN_SIZE;
	private Supplier<String> extraMessageSupplier = () -> EMPTY_STRING;
	private ProgressBar progressBar;

	public ProgressStepExecutionListener(ProgressBarBuilder builder) {
		this.builder = builder;
	}

	public void setInitialMaxSupplier(LongSupplier supplier) {
		this.initialMaxSupplier = supplier;
	}

	public void setExtraMessageSupplier(Supplier<String> supplier) {
		this.extraMessageSupplier = supplier;
	}

	@Override
	public void beforeStep(StepExecution stepExecution) {
		progressBar = builder.build();
		try {
			progressBar.maxHint(initialMaxSupplier.getAsLong());
		} catch (Exception e) {
			log.error("Could not estimate size", e);
		}
	}

	@Override
	public ExitStatus afterStep(StepExecution stepExecution) {
		if (!stepExecution.getStatus().isUnsuccessful()) {
			progressBar.stepTo(progressBar.getMax());
		}
		progressBar.close();
		return stepExecution.getExitStatus();
	}

	@Override
	public void afterWrite(Chunk items) {
		progressBar.stepBy(items.size());
		progressBar.setExtraMessage(extraMessageSupplier.get());
	}

	@Override
	public void onWriteError(Exception exception, Chunk items) {
		progressBar.stepBy(items.size());

	}

}
