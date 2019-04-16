package com.redislabs.riot.cli.redis;

import java.text.NumberFormat;
import java.util.Map;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.beans.factory.InitializingBean;

import com.redislabs.riot.batch.BatchConfig;
import com.redislabs.riot.batch.BatchOptions;
import com.redislabs.riot.cli.AbstractImportSubCommand;
import com.redislabs.riot.cli.HelpAwareCommand;
import com.redislabs.riot.redis.writer.AbstractRedisWriter;

import picocli.CommandLine.ParentCommand;

public abstract class AbstractRedisImportSubSubCommand extends HelpAwareCommand {

	@ParentCommand
	private AbstractImportSubCommand parent;

	NumberFormat numberFormat = NumberFormat.getIntegerInstance();

	@Override
	public Void call() throws Exception {
		BatchConfig batch = new BatchConfig();
		batch.afterPropertiesSet();
		ItemStreamReader<Map<String, Object>> reader = parent.reader();
		Job job = batch.importJob(new BatchOptions(), reader, null, writer());
		long startTime = System.currentTimeMillis();
		System.out.println("Importing into " + getTargetDescription() + " from " + parent.getSourceDescription());
		JobExecution execution = batch.launch(job);
		long endTime = System.currentTimeMillis();
		long duration = endTime - startTime;
		for (StepExecution stepExecution : execution.getStepExecutions()) {
			double durationInSeconds = (double) duration / 1000;
			int writeCount = stepExecution.getWriteCount();
			double throughput = writeCount / durationInSeconds;
			System.out.println("Imported " + numberFormat.format(writeCount) + " items in " + durationInSeconds
					+ " seconds (" + numberFormat.format(throughput) + " writes/sec)");
		}
		System.out.println("Executed import with status " + execution.getExitStatus().getExitCode());
		return null;
	}

	public abstract String getTargetDescription();

	private AbstractRedisWriter writer() throws Exception {
		AbstractRedisWriter writer = createWriter();
		writer.setPool(parent.getParent().getPool());
		if (writer instanceof InitializingBean) {
			((InitializingBean) writer).afterPropertiesSet();
		}
		return writer;
	}

	protected abstract AbstractRedisWriter createWriter();

}
