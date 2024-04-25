package com.redis.riot.cli;

import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
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
public class ProgressStepExecutionListener implements StepExecutionListener, ItemWriteListener {

	private final ProgressBarBuilder builder;

	private LongSupplier initialMax;
	private Supplier<String> extraMessage;

	private ProgressBar progressBar;

	public ProgressStepExecutionListener(ProgressBarBuilder builder) {
		this.builder = builder;
	}

	public void setInitialMax(LongSupplier supplier) {
		this.initialMax = supplier;
	}

	public void setExtraMessage(Supplier<String> supplier) {
		this.extraMessage = supplier;
	}

	@Override
	public void beforeStep(StepExecution stepExecution) {
		if (initialMax != null) {
			builder.setInitialMax(initialMax.getAsLong());
		}
		progressBar = builder.build();
	}

	@Override
	public void afterWrite(Chunk items) {
		if (progressBar != null) {
			progressBar.stepBy(items.size());
			if (extraMessage != null) {
				progressBar.setExtraMessage(extraMessage.get());
			}
		}
	}

	@Override
	public ExitStatus afterStep(StepExecution stepExecution) {
		if (progressBar != null) {
			if (!stepExecution.getStatus().isUnsuccessful()) {
				progressBar.stepTo(progressBar.getMax());
			}
			progressBar.close();
			progressBar = null;
		}
		return stepExecution.getExitStatus();
	}

}
