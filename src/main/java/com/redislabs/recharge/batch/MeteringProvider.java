package com.redislabs.recharge.batch;

import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.LongTaskTimer.Sample;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

@Component
public class MeteringProvider {

	private static final String SEPARATOR = ".";
	private static final String TAG_NAME = "name";
	@Autowired
	private MeterRegistry registry;
	private Map<String, Sample> longSamples = new HashMap<>();
	private Map<String, Timer.Sample> shortSamples = new HashMap<>();

	public void startLongTaskTimer(JobExecution jobExecution) {
		String jobName = getName(jobExecution);
		LongTaskTimer timer = LongTaskTimer.builder("job.execution").tag(TAG_NAME, jobName).register(registry);
		longSamples.put(jobName, timer.start());
	}

	public void stop(JobExecution jobExecution) {
		stop(getName(jobExecution));
	}

	public void startLongTaskTimer(StepExecution stepExecution) {
		String stepName = getName(stepExecution);
		LongTaskTimer timer = LongTaskTimer.builder("step.execution").tag(TAG_NAME, stepName).register(registry);
		longSamples.put(stepName, timer.start());
	}

	public void stop(StepExecution stepExecution) {
		stop(getName(stepExecution));
	}

	private void stop(String name) {
		longSamples.get(name).stop();
	}

	private String getName(JobExecution jobExecution) {
		return jobExecution.getJobInstance().getJobName();
	}

	private String getName(StepExecution stepExecution) {
		String jobName = stepExecution.getJobExecution().getJobInstance().getJobName();
		String stepName = stepExecution.getStepName();
		return join(jobName, stepName);
	}

	private String join(String... values) {
		return String.join(SEPARATOR, values);
	}

	public void startTimer(String metricName, String tagName) {
		shortSamples.put(join(metricName, tagName), Timer.start(registry));
	}

	public void stopTimer(String writerName, String tagName) {
		Timer timer = registry.timer(writerName, TAG_NAME, tagName);
		shortSamples.get(join(writerName, tagName)).stop(timer);
	}

}
