package com.redis.riot;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;

import com.redis.spring.batch.item.redis.reader.KeyComparison.Status;
import com.redis.spring.batch.item.redis.reader.KeyComparisonStat;
import com.redis.spring.batch.item.redis.reader.KeyComparisonStats;

public class CompareStepListener implements StepExecutionListener {

	private static final String STATUS_COUNT = "%s %d";

	private final KeyComparisonStats stats;

	public CompareStepListener(KeyComparisonStats stats) {
		this.stats = stats;
	}

	@Override
	public ExitStatus afterStep(StepExecution stepExecution) {
		if (stepExecution.getStatus().isUnsuccessful()) {
			return null;
		}
		List<KeyComparisonStat> mismatches = stats.allStats().stream()
				.filter(s -> s.getStatus() != Status.OK && s.getCount() > 0).collect(Collectors.toList());
		if (mismatches.isEmpty()) {
			return ExitStatus.COMPLETED;
		}
		return new ExitStatus(ExitStatus.FAILED.getExitCode(), verificationFailedExitDescription());
	}

	protected String verificationFailedExitDescription() {
		StringBuilder builder = new StringBuilder();
		builder.append("Verification failed:");
		for (Entry<Status, List<KeyComparisonStat>> entry : statsByStatus(stats)) {
			Long count = entry.getValue().stream().collect(Collectors.summingLong(KeyComparisonStat::getCount));
			builder.append(System.lineSeparator());
			builder.append(String.format(STATUS_COUNT, entry.getKey(), count));
		}
		return builder.toString();
	}

	public static Set<Entry<Status, List<KeyComparisonStat>>> statsByStatus(KeyComparisonStats stats) {
		Map<Status, List<KeyComparisonStat>> map = stats.allStats().stream()
				.collect(Collectors.groupingBy(KeyComparisonStat::getStatus));
		List<Status> statuses = new ArrayList<>(map.keySet());
		statuses.sort(CompareStepListener::compareStatus);
		Map<Status, List<KeyComparisonStat>> orderedMap = new LinkedHashMap<>();
		statuses.forEach(s -> orderedMap.put(s, map.get(s)));
		return orderedMap.entrySet();
	}

	private static int compareStatus(Status status1, Status status2) {
		return Arrays.binarySearch(Status.values(), status1) - Arrays.binarySearch(Status.values(), status2);
	}

}
