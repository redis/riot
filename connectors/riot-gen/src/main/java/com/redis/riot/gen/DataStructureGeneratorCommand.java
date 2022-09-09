package com.redis.riot.gen;

import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.springframework.batch.core.Job;
import org.springframework.batch.item.ItemReader;

import com.redis.riot.AbstractTransferCommand;
import com.redis.riot.JobCommandContext;
import com.redis.riot.ProgressMonitor;
import com.redis.riot.RedisWriterOptions;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.DataStructure.Type;
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
	protected Job job(JobCommandContext context) throws Exception {
		RedisItemWriter<String, String, DataStructure<String>> writer = RedisItemWriter.dataStructure(context.pool())
				.options(writerOptions.writerOptions()).build();
		log.log(Level.FINE, "Creating random data structure reader with {0}", options);
		ProgressMonitor monitor = options.configure(progressMonitor()).task("Generating").build();
		return job(context, NAME, step(context, NAME, reader(), null, writer), monitor);
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
		options.configureReader(reader);
		Optional<Long> sleep = options.getSleep();
		if (sleep.isPresent()) {
			return new ThrottledItemReader<>(reader.build(), sleep.get());
		}
		return reader.build();
	}

}
