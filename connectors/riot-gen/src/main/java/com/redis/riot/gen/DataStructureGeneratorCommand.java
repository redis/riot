package com.redis.riot.gen;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.item.ItemReader;

import com.redis.riot.AbstractTransferCommand;
import com.redis.riot.RedisWriterOptions;
import com.redis.riot.RiotStep;
import com.redis.spring.batch.DataStructure;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.reader.RandomDataStructureItemReader;
import com.redis.spring.batch.support.Range;

import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "ds", description = "Import randomly-generated data structures")
public class DataStructureGeneratorCommand extends AbstractTransferCommand {

	private static final Logger log = LoggerFactory.getLogger(DataStructureGeneratorCommand.class);

	private static final String NAME = "random-import";

	@CommandLine.Mixin
	private DataStructureGeneratorOptions options = new DataStructureGeneratorOptions();

	@ArgGroup(exclusive = false, heading = "Writer options%n")
	private RedisWriterOptions writerOptions = new RedisWriterOptions();

	@Override
	protected Job job(JobBuilder jobBuilder) throws Exception {
		RedisItemWriter<String, String, DataStructure<String>> writer = writerOptions
				.configureWriter(
						RedisItemWriter.client(getRedisOptions().client()).string().dataStructure().xaddArgs(m -> null))
				.build();
		log.debug("Creating random data structure reader with {}", options);
		return jobBuilder.start(step(RiotStep.reader(reader()).writer(writer).name(NAME).taskName("Generating")
				.max(() -> (long) options.getCount()).build()).build()).build();
	}

	private ItemReader<DataStructure<String>> reader() {
		RandomDataStructureItemReader.Builder reader = RandomDataStructureItemReader.builder().start(options.getStart())
				.count(options.getCount())
				.collectionCardinality(
						Range.between(options.getMinCollectionCardinality(), options.getMaxCollectionCardinality()))
				.keyspace(options.getKeyspace())
				.stringValueSize(Range.between(options.getMinStringSize(), options.getMaxStringSize()))
				.types(options.getTypes())
				.zsetScore(Range.between(options.getMinZsetScore(), options.getMaxZsetScore()));
		if (options.getMinExpiration().isPresent() && options.getMaxExpiration().isPresent()) {
			reader.expiration(
					Range.between(options.getMinExpiration().getAsInt(), options.getMaxExpiration().getAsInt()));
		}
		if (options.getSleep() > 0) {
			return new ThrottledItemReader<>(reader.build(), options.getSleep());
		}
		return reader.build();
	}

}
