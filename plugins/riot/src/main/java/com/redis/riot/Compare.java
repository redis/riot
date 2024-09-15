package com.redis.riot;

import org.springframework.batch.core.Job;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "compare", description = "Compare two Redis databases.")
public class Compare extends AbstractCompareCommand {

	@Option(names = "--stream-msg-id", description = "Compare stream message ids. True by default.", negatable = true, defaultValue = "true", fallbackValue = "true")
	private boolean compareStreamMessageId = DEFAULT_COMPARE_STREAM_MESSAGE_ID;

	@Option(names = "--quick", description = "Skip value comparison.")
	private boolean quick;

	@Override
	protected boolean isStruct() {
		return true;
	}

	@Override
	protected boolean isQuickCompare() {
		return quick;
	}

	@Override
	protected boolean isIgnoreStreamMessageId() {
		return !compareStreamMessageId;
	}

	@Override
	protected Job job() {
		return job(compareStep());
	}

	public boolean isCompareStreamMessageId() {
		return compareStreamMessageId;
	}

	public void setCompareStreamMessageId(boolean streamMessageId) {
		this.compareStreamMessageId = streamMessageId;
	}

}
