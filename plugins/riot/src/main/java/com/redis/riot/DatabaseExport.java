package com.redis.riot;

import java.util.Map;

import org.springframework.batch.core.Job;
import org.springframework.batch.item.ItemProcessor;

import com.redis.riot.core.EvaluationContextArgs;
import com.redis.riot.db.DatabaseArgs;
import com.redis.riot.db.DatabaseWriterArgs;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.reader.MemKeyValue;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "db-export", description = "Export Redis data to a relational database.")
public class DatabaseExport extends AbstractExport {

	@ArgGroup(exclusive = false)
	private DatabaseWriterArgs databaseWriterArgs = new DatabaseWriterArgs();

	@ArgGroup(exclusive = false)
	private EvaluationContextArgs evaluationContextArgs = new EvaluationContextArgs();

	@ArgGroup(exclusive = false)
	private KeyValueMapProcessorArgs processorArgs = new KeyValueMapProcessorArgs();

	public void copyTo(DatabaseExport target) {
		super.copyTo(target);
		target.databaseWriterArgs = databaseWriterArgs;
		target.evaluationContextArgs = evaluationContextArgs;
		target.processorArgs = processorArgs;
	}

	@Override
	protected Job job() {
		return job(exportStep(reader(), databaseWriterArgs.writer()).processor(processor()).taskName("Exporting"));
	}

	private ItemProcessor<? super MemKeyValue<String, Object>, ? extends Map<String, Object>> processor() {
		return processorArgs.processor(evaluationContext(evaluationContextArgs));
	}

	private RedisItemReader<String, String, MemKeyValue<String, Object>> reader() {
		RedisItemReader<String, String, MemKeyValue<String, Object>> reader = RedisItemReader.struct();
		configure(reader);
		return reader;
	}

	public DatabaseArgs getDatabaseWriterArgs() {
		return databaseWriterArgs;
	}

	public void setDatabaseWriterArgs(DatabaseWriterArgs args) {
		this.databaseWriterArgs = args;
	}

	public KeyValueMapProcessorArgs getProcessorArgs() {
		return processorArgs;
	}

	public void setProcessorArgs(KeyValueMapProcessorArgs args) {
		this.processorArgs = args;
	}

}
