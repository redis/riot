package com.redis.riot;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import com.redis.riot.core.QuietMapAccessor;
import com.redis.riot.core.RiotUtils;
import com.redis.riot.operation.DelCommand;
import com.redis.riot.operation.ExpireCommand;
import com.redis.riot.operation.GeoaddCommand;
import com.redis.riot.operation.HsetCommand;
import com.redis.riot.operation.JsonSetCommand;
import com.redis.riot.operation.LpushCommand;
import com.redis.riot.operation.OperationCommand;
import com.redis.riot.operation.RpushCommand;
import com.redis.riot.operation.SaddCommand;
import com.redis.riot.operation.SetCommand;
import com.redis.riot.operation.SugaddCommand;
import com.redis.riot.operation.TsAddCommand;
import com.redis.riot.operation.XaddCommand;
import com.redis.riot.operation.ZaddCommand;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.item.redis.common.Operation;

import io.lettuce.core.codec.StringCodec;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(subcommands = { ExpireCommand.class, DelCommand.class, GeoaddCommand.class, HsetCommand.class,
		LpushCommand.class, RpushCommand.class, SaddCommand.class, SetCommand.class, XaddCommand.class,
		ZaddCommand.class, SugaddCommand.class, JsonSetCommand.class,
		TsAddCommand.class }, subcommandsRepeatable = true, synopsisSubcommandLabel = "[REDIS COMMAND...]", commandListHeading = "Redis commands:%n")
public abstract class AbstractImport extends AbstractRedisCommand {

	@ArgGroup(exclusive = false, heading = "Redis writer options%n")
	private RedisWriterArgs redisWriterArgs = new RedisWriterArgs();

	/**
	 * Initialized manually during command parsing
	 */
	private List<OperationCommand> importOperationCommands = new ArrayList<>();

	public void copyTo(AbstractImport target) {
		super.copyTo(target);
		target.redisWriterArgs = redisWriterArgs;
		target.importOperationCommands = importOperationCommands;
	}

	protected List<Operation<String, String, Map<String, Object>, Object>> operations() {
		return importOperationCommands.stream().map(OperationCommand::operation).collect(Collectors.toList());
	}

	protected boolean hasOperations() {
		return !CollectionUtils.isEmpty(importOperationCommands);
	}

	protected void assertHasOperations() {
		Assert.isTrue(hasOperations(), "No Redis command specified");
	}

	protected ItemWriter<Map<String, Object>> mapWriter() {
		assertHasOperations();
		return RiotUtils.writer(operations().stream().map(this::writer).collect(Collectors.toList()));
	}

	@Override
	protected void configure(RedisItemWriter<?, ?, ?> writer) {
		log.info("Configuring Redis writer with {}", redisWriterArgs);
		redisWriterArgs.configure(writer);
		super.configure(writer);
	}

	protected <T> ItemWriter<T> writer(Operation<String, String, T, Object> operation) {
		RedisItemWriter<String, String, T> writer = new RedisItemWriter<>(StringCodec.UTF8, operation);
		configure(writer);
		return writer;
	}

	protected ItemProcessor<Map<String, Object>, Map<String, Object>> mapProcessor(ImportProcessorArgs args) {
		StandardEvaluationContext evaluationContext = evaluationContext(args.getEvaluationContextArgs());
		evaluationContext.addPropertyAccessor(new QuietMapAccessor());
		log.info("Creating processor with {}", args);
		return args.getMapProcessorArgs().processor(evaluationContext);
	}

	public RedisWriterArgs getRedisWriterArgs() {
		return redisWriterArgs;
	}

	public void setRedisWriterArgs(RedisWriterArgs redisWriterArgs) {
		this.redisWriterArgs = redisWriterArgs;
	}

	public List<OperationCommand> getImportOperationCommands() {
		return importOperationCommands;
	}

	public void setImportOperationCommands(OperationCommand... commands) {
		setImportOperationCommands(Arrays.asList(commands));
	}

	public void setImportOperationCommands(List<OperationCommand> commands) {
		this.importOperationCommands = commands;
	}

}
