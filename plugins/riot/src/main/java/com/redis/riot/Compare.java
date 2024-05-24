package com.redis.riot;

import java.time.Duration;

import org.springframework.batch.core.Job;

import com.redis.riot.core.Step;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.reader.DefaultKeyComparator;
import com.redis.spring.batch.item.redis.reader.KeyComparator;
import com.redis.spring.batch.item.redis.reader.KeyComparison;
import com.redis.spring.batch.item.redis.reader.KeyComparison.Status;
import com.redis.spring.batch.item.redis.reader.KeyComparisonItemReader;
import com.redis.spring.batch.item.redis.reader.MemKeyValue;

import io.lettuce.core.codec.ByteArrayCodec;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "compare", description = "Compare two Redis databases.")
public class Compare extends AbstractTargetExport {

	public static final int DEFAULT_TARGET_POOL_SIZE = RedisItemReader.DEFAULT_POOL_SIZE;
	public static final String STEP = "compare-step";
	public static final boolean DEFAULT_COMPARE_STREAM_MESSAGE_ID = true;

	private static final Status[] STATUSES = { Status.OK, Status.MISSING, Status.TYPE, Status.VALUE, Status.TTL };
	private static final String NUMBER_FORMAT = "%,d";
	private static final String COMPARE_MESSAGE = compareMessageFormat();

	@ArgGroup(exclusive = false)
	private CompareArgs compareArgs = new CompareArgs();

	@Option(names = "--stream-msg-id", description = "Compare stream message ids. True by default.", negatable = true, defaultValue = "true", fallbackValue = "true")
	private boolean compareStreamMessageId = DEFAULT_COMPARE_STREAM_MESSAGE_ID;

	public void copyTo(Compare target) {
		super.copyTo(target);
		target.compareArgs = compareArgs;
		target.compareStreamMessageId = compareStreamMessageId;
	}

	@Override
	protected Job job() {
		return job(compareStep());
	}

	public Step<KeyComparison<byte[]>, KeyComparison<byte[]>> compareStep() {
		KeyComparisonItemReader<byte[], byte[]> reader = reader();
		CompareStatusCountItemWriter<byte[]> writer = new CompareStatusCountItemWriter<>();
		Step<KeyComparison<byte[]>, KeyComparison<byte[]>> step = new Step<>(reader, writer).name(STEP);
		step.taskName("Comparing");
		step.statusMessageSupplier(() -> String.format(COMPARE_MESSAGE, writer.getCounts(STATUSES).toArray()));
		step.maxItemCountSupplier(scanSizeEstimator(reader.getSourceReader()));
		if (compareArgs.isShowDiffs()) {
			log.info("Adding key diff logger");
			step.writeListener(new CompareDiffLogger<>(ByteArrayCodec.INSTANCE));
		}
		step.executionListener(new CompareSummaryLogger(writer));
		return step;
	}

	private KeyComparisonItemReader<byte[], byte[]> reader() {
		RedisItemReader<byte[], byte[], MemKeyValue<byte[], Object>> source = redisReader();
		configure(source);
		RedisItemReader<byte[], byte[], MemKeyValue<byte[], Object>> target = redisReader();
		configureTarget(target);
		if (compareArgs.getTargetReadFrom() != null) {
			log.info("Configuring key comparison target Redis reader with read-from {}",
					compareArgs.getTargetReadFrom());
			target.setReadFrom(compareArgs.getTargetReadFrom().getReadFrom());
		}
		log.info("Creating key comparison reader with {}", compareArgs);
		KeyComparisonItemReader<byte[], byte[]> reader = new KeyComparisonItemReader<>(source, target);
		reader.setComparator(keyComparator());
		return reader;

	}

	private RedisItemReader<byte[], byte[], MemKeyValue<byte[], Object>> redisReader() {
		if (compareArgs.getMode() == CompareMode.QUICK) {
			return RedisItemReader.type(ByteArrayCodec.INSTANCE);
		}
		return RedisItemReader.struct(ByteArrayCodec.INSTANCE);
	}

	private static String compareMessageFormat() {
		StringBuilder builder = new StringBuilder();
		for (Status status : STATUSES) {
			builder.append(String.format(" | %s: %s", status.name().toLowerCase(), NUMBER_FORMAT));
		}
		return builder.toString();
	}

	public KeyComparator<byte[], byte[]> keyComparator() {
		DefaultKeyComparator<byte[], byte[]> comparator = new DefaultKeyComparator<>();
		comparator.setTtlTolerance(Duration.ofMillis(compareArgs.getTtlTolerance()));
		comparator.setIgnoreStreamMessageId(!compareStreamMessageId);
		return comparator;
	}

	public CompareArgs getCompareArgs() {
		return compareArgs;
	}

	public void setCompareArgs(CompareArgs args) {
		this.compareArgs = args;
	}

	public boolean isCompareStreamMessageId() {
		return compareStreamMessageId;
	}

	public void setCompareStreamMessageId(boolean streamMessageId) {
		this.compareStreamMessageId = streamMessageId;
	}

}
