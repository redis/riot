package com.redis.riot.gen;

import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.item.ItemReader;

import com.redis.riot.AbstractTransferCommand;
import com.redis.riot.RedisWriterOptions;
import com.redis.riot.RiotStep;
import com.redis.spring.batch.DataStructure;
import com.redis.spring.batch.DataStructure.Type;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.reader.DataStructureGeneratorItemReader;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

@Command(name = "ds", description = "Import randomly-generated data structures")
public class DataStructureGeneratorCommand extends AbstractTransferCommand {

	private static final Logger log = Logger.getLogger(DataStructureGeneratorCommand.class.getName());

	private static final String NAME = "random-import";

	@Mixin
	private DataStructureGeneratorOptions options = new DataStructureGeneratorOptions();

	@ArgGroup(exclusive = false, heading = "Writer options%n")
	private RedisWriterOptions writerOptions = new RedisWriterOptions();

	@Override
	protected Job job(JobBuilder jobBuilder) throws Exception {
		RedisItemWriter<String, String, DataStructure<String>> writer = writerOptions
				.configureWriter(
						RedisItemWriter.client(getRedisOptions().client()).string().dataStructure().xaddArgs(m -> null))
				.build();
		log.log(Level.FINE, "Creating random data structure reader with {0}", options);
		return jobBuilder.start(step(RiotStep.reader(reader()).writer(writer).name(NAME).taskName("Generating")
				.max(() -> (long) options.getCount()).build()).build()).build();
	}

	private ItemReader<DataStructure<String>> reader() {
		DataStructureGeneratorItemReader.Builder reader = DataStructureGeneratorItemReader.builder()
				.currentItemCount(options.getStart() - 1).maxItemCount(options.getCount())
				.streamSize(options.getStreamSize()).streamFieldCount(options.getStreamFieldCount())
				.streamFieldSize(options.getStreamFieldSize()).listSize(options.getListSize())
				.setSize(options.getSetSize()).zsetSize(options.getZsetSize())
				.timeseriesSize(options.getTimeseriesSize()).keyspace(options.getKeyspace())
				.stringSize(options.getStringSize()).types(options.getTypes().toArray(Type[]::new))
				.zsetScore(options.getZsetScore()).hashSize(options.getHashSize())
				.hashFieldSize(options.getHashFieldSize()).jsonFieldCount(options.getJsonSize())
				.jsonFieldSize(options.getJsonFieldSize());
		options.getTimeseriesStartTime().ifPresent(t -> reader.timeseriesStartTime(t.toEpochMilli()));
		options.getExpiration().ifPresent(reader::expiration);
		Optional<Long> sleep = options.getSleep();
		if (sleep.isPresent() && sleep.get() > 0) {
			return new ThrottledItemReader<>(reader.build(), sleep.get());
		}
		return reader.build();
	}

}
