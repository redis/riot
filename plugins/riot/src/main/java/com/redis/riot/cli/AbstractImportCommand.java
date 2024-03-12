package com.redis.riot.cli;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.expression.Expression;

import com.redis.lettucemod.timeseries.DuplicatePolicy;
import com.redis.riot.cli.AbstractImportCommand.DelCommand;
import com.redis.riot.cli.AbstractImportCommand.ExpireCommand;
import com.redis.riot.cli.AbstractImportCommand.GeoaddCommand;
import com.redis.riot.cli.AbstractImportCommand.HsetCommand;
import com.redis.riot.cli.AbstractImportCommand.JsonSetCommand;
import com.redis.riot.cli.AbstractImportCommand.LpushCommand;
import com.redis.riot.cli.AbstractImportCommand.RpushCommand;
import com.redis.riot.cli.AbstractImportCommand.SaddCommand;
import com.redis.riot.cli.AbstractImportCommand.SetCommand;
import com.redis.riot.cli.AbstractImportCommand.SugaddCommand;
import com.redis.riot.cli.AbstractImportCommand.TsAddCommand;
import com.redis.riot.cli.AbstractImportCommand.XaddCommand;
import com.redis.riot.cli.AbstractImportCommand.ZaddCommand;
import com.redis.riot.core.AbstractImport;
import com.redis.riot.core.RiotStep;
import com.redis.riot.core.operation.AbstractCollectionMapOperationBuilder;
import com.redis.riot.core.operation.AbstractFilterMapOperationBuilder;
import com.redis.riot.core.operation.AbstractMapOperationBuilder;
import com.redis.riot.core.operation.DelBuilder;
import com.redis.riot.core.operation.ExpireBuilder;
import com.redis.riot.core.operation.GeoaddBuilder;
import com.redis.riot.core.operation.HsetBuilder;
import com.redis.riot.core.operation.JsonSetBuilder;
import com.redis.riot.core.operation.LpushBuilder;
import com.redis.riot.core.operation.RpushBuilder;
import com.redis.riot.core.operation.SaddBuilder;
import com.redis.riot.core.operation.SetBuilder;
import com.redis.riot.core.operation.SetBuilder.StringFormat;
import com.redis.riot.core.operation.SugaddBuilder;
import com.redis.riot.core.operation.TsAddBuilder;
import com.redis.riot.core.operation.XaddSupplier;
import com.redis.riot.core.operation.ZaddSupplier;
import com.redis.spring.batch.common.Operation;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

@Command(subcommands = { ExpireCommand.class, DelCommand.class, GeoaddCommand.class, HsetCommand.class,
		LpushCommand.class, RpushCommand.class, SaddCommand.class, SetCommand.class, XaddCommand.class,
		ZaddCommand.class, SugaddCommand.class, JsonSetCommand.class,
		TsAddCommand.class }, subcommandsRepeatable = true, synopsisSubcommandLabel = "[REDIS COMMAND...]", commandListHeading = "Redis commands:%n")
public abstract class AbstractImportCommand extends AbstractJobCommand {

	@Option(arity = "1..*", names = "--proc", description = "SpEL expressions in the form field1=\"exp\" field2=\"exp\"...", paramLabel = "<f=exp>")
	Map<String, Expression> processorExpressions;

	@Option(names = "--filter", description = "Discard records using a SpEL expression.", paramLabel = "<exp>")
	Expression filter;

	/**
	 * Initialized manually during command parsing
	 */
	private List<OperationCommand> commands = new ArrayList<>();

	public List<OperationCommand> getCommands() {
		return commands;
	}

	public void setCommands(List<OperationCommand> commands) {
		this.commands = commands;
	}

	protected List<Operation<String, String, Map<String, Object>, Object>> operations() {
		return commands.stream().map(OperationCommand::operation).collect(Collectors.toList());
	}

	@Override
	protected AbstractImport jobExecutable() {
		AbstractImport executable = importExecutable();
		executable.setOperations(operations());
		executable.setProcessorExpressions(processorExpressions);
		executable.setFilterExpression(filter);
		return executable;
	}

	protected abstract AbstractImport importExecutable();

	@Override
	protected String taskName(RiotStep<?, ?> step) {
		return "Importing";
	}

	protected static class FieldFilteringArgs {

		@Option(arity = "1..*", names = "--include", description = "Fields to include.", paramLabel = "<field>")
		List<String> includes;

		@Option(arity = "1..*", names = "--exclude", description = "Fields to exclude.", paramLabel = "<field>")
		List<String> excludes;

		public void configure(AbstractFilterMapOperationBuilder builder) {
			builder.setIncludes(includes);
			builder.setExcludes(excludes);
		}

	}

	@Command(name = "del", description = "Delete keys")
	public static class DelCommand extends OperationCommand {

		@Override
		protected DelBuilder operationBuilder() {
			return new DelBuilder();
		}

	}

	@Command(name = "expire", description = "Set timeouts on keys")
	public static class ExpireCommand extends OperationCommand {

		public static final long DEFAULT_TTL = 60;

		@Option(names = "--ttl", description = "EXPIRE timeout field.", paramLabel = "<field>")
		private String ttlField;

		@Option(names = "--ttl-default", description = "EXPIRE default timeout (default: ${DEFAULT-VALUE}).", paramLabel = "<sec>")
		private long defaultTtl = DEFAULT_TTL;

		@Override
		protected ExpireBuilder operationBuilder() {
			ExpireBuilder builder = new ExpireBuilder();
			builder.setTtlField(ttlField);
			builder.setDefaultTtl(Duration.ofSeconds(defaultTtl));
			return builder;
		}

	}

	@Command(name = "geoadd", description = "Add members to a geo set")
	public static class GeoaddCommand extends AbstractCollectionCommand {

		@Option(names = "--lon", required = true, description = "Longitude field.", paramLabel = "<field>")
		private String longitude;

		@Option(names = "--lat", required = true, description = "Latitude field.", paramLabel = "<field>")
		private String latitude;

		@Override
		protected GeoaddBuilder collectionOperationBuilder() {
			GeoaddBuilder builder = new GeoaddBuilder();
			builder.setLatitude(latitude);
			builder.setLongitude(longitude);
			return builder;
		}

	}

	@Command(name = "hset", aliases = "hmset", description = "Set hashes from input")
	public static class HsetCommand extends OperationCommand {

		@Mixin
		private FieldFilteringArgs filteringArgs = new FieldFilteringArgs();

		@Override
		protected HsetBuilder operationBuilder() {
			HsetBuilder builder = new HsetBuilder();
			filteringArgs.configure(builder);
			return builder;
		}

	}

	@Command(name = "json.set", description = "Add JSON documents to RedisJSON")
	public static class JsonSetCommand extends OperationCommand {

		@Option(names = "--path", description = "Path field.", paramLabel = "<field>")
		private String path;

		@Override
		protected JsonSetBuilder operationBuilder() {
			JsonSetBuilder supplier = new JsonSetBuilder();
			supplier.setPath(path);
			return supplier;
		}

	}

	@Command(name = "lpush", description = "Insert values at the head of a list")
	public static class LpushCommand extends AbstractCollectionCommand {

		@Override
		protected LpushBuilder collectionOperationBuilder() {
			return new LpushBuilder();
		}

	}

	@Command(name = "rpush", description = "Insert values at the tail of a list")
	public static class RpushCommand extends AbstractCollectionCommand {

		@Override
		protected RpushBuilder collectionOperationBuilder() {
			return new RpushBuilder();
		}

	}

	@Command(name = "sadd", description = "Add members to a set")
	public static class SaddCommand extends AbstractCollectionCommand {

		@Override
		protected SaddBuilder collectionOperationBuilder() {
			return new SaddBuilder();
		}

	}

	@Command(name = "set", description = "Set strings from input")
	public static class SetCommand extends OperationCommand {

		public static final StringFormat DEFAULT_FORMAT = StringFormat.JSON;

		@Option(names = "--format", description = "Serialization: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<fmt>")
		private StringFormat format = DEFAULT_FORMAT;

		@Option(names = "--field", description = "Raw value field.", paramLabel = "<field>")
		private String field;

		@Option(names = "--root", description = "XML root element name.", paramLabel = "<name>")
		private String root;

		@Override
		protected SetBuilder operationBuilder() {
			SetBuilder supplier = new SetBuilder();
			supplier.setField(field);
			supplier.setFormat(format);
			supplier.setRoot(root);
			return supplier;
		}

	}

	@Command(name = "ft.sugadd", description = "Add suggestion strings to a RediSearch auto-complete dictionary")
	public static class SugaddCommand extends OperationCommand {

		public static final double DEFAULT_SCORE = 1;

		public static final boolean DEFAULT_INCREMENT = false;

		@Option(names = "--field", required = true, description = "Field containing the strings to add.", paramLabel = "<field>")
		private String stringField;

		@Option(names = "--score", description = "Name of the field to use for scores.", paramLabel = "<field>")
		private String scoreField;

		@Option(names = "--score-default", description = "Score when field not present (default: ${DEFAULT-VALUE}).", paramLabel = "<num>")
		private double defaultScore = DEFAULT_SCORE;

		@Option(names = "--payload", description = "Field containing the payload.", paramLabel = "<field>")
		private String payloadField;

		@Option(names = "--increment", description = "Increment the existing suggestion by the score instead of replacing the score.")
		private boolean increment = DEFAULT_INCREMENT;

		public String getStringField() {
			return stringField;
		}

		public void setStringField(String field) {
			this.stringField = field;
		}

		public String getScoreField() {
			return scoreField;
		}

		public void setScore(String field) {
			this.scoreField = field;
		}

		public double getDefaultScore() {
			return defaultScore;
		}

		public void setDefaultScore(double scoreDefault) {
			this.defaultScore = scoreDefault;
		}

		public String getPayloadField() {
			return payloadField;
		}

		public void setPayload(String field) {
			this.payloadField = field;
		}

		public boolean isIncrement() {
			return increment;
		}

		public void setIncrement(boolean increment) {
			this.increment = increment;
		}

		@Override
		protected SugaddBuilder operationBuilder() {
			SugaddBuilder supplier = new SugaddBuilder();
			supplier.setDefaultScore(defaultScore);
			supplier.setIncrement(increment);
			supplier.setStringField(stringField);
			supplier.setPayloadField(payloadField);
			supplier.setScoreField(scoreField);
			return supplier;
		}

	}

	@Command(name = "ts.add", description = "Add samples to RedisTimeSeries")
	public static class TsAddCommand extends OperationCommand {

		@Option(names = "--timestamp", description = "Name of the field to use for timestamps. If unset, uses auto-timestamping.", paramLabel = "<field>")
		private String timestampField;

		@Option(names = "--value", required = true, description = "Name of the field to use for values.", paramLabel = "<field>")
		private String valueField;

		@Option(names = "--on-duplicate", description = "Duplicate policy: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<name>")
		private DuplicatePolicy duplicatePolicy = TsAddBuilder.DEFAULT_DUPLICATE_POLICY;

		@Option(arity = "1..*", names = "--labels", description = "Labels in the form label1=field1 label2=field2...", paramLabel = "SPEL")
		private Map<String, String> labels = new LinkedHashMap<>();

		@Override
		protected TsAddBuilder operationBuilder() {
			TsAddBuilder supplier = new TsAddBuilder();
			supplier.setDuplicatePolicy(duplicatePolicy);
			supplier.setTimestampField(timestampField);
			supplier.setValueField(valueField);
			supplier.setLabels(labels);
			return supplier;
		}

	}

	@Command(name = "xadd", description = "Append entries to a stream")
	public static class XaddCommand extends OperationCommand {

		@Mixin
		private FieldFilteringArgs filteringOptions = new FieldFilteringArgs();

		@Option(names = "--maxlen", description = "Stream maxlen.", paramLabel = "<int>")
		private long maxlen;

		@Option(names = "--trim", description = "Stream efficient trimming ('~' flag).")
		private boolean approximateTrimming;

		public FieldFilteringArgs getFilteringOptions() {
			return filteringOptions;
		}

		public void setFilteringOptions(FieldFilteringArgs filteringOptions) {
			this.filteringOptions = filteringOptions;
		}

		public long getMaxlen() {
			return maxlen;
		}

		public void setMaxlen(long maxlen) {
			this.maxlen = maxlen;
		}

		public boolean isApproximateTrimming() {
			return approximateTrimming;
		}

		public void setApproximateTrimming(boolean approximateTrimming) {
			this.approximateTrimming = approximateTrimming;
		}

		@Override
		protected XaddSupplier operationBuilder() {
			XaddSupplier supplier = new XaddSupplier();
			supplier.setApproximateTrimming(approximateTrimming);
			supplier.setMaxlen(maxlen);
			return supplier;
		}

	}

	@Command(name = "zadd", description = "Add members with scores to a sorted set")
	public static class ZaddCommand extends AbstractCollectionCommand {

		@Option(names = "--score", description = "Name of the field to use for scores.", paramLabel = "<field>")
		private String scoreField;

		@Option(names = "--score-default", description = "Score when field not present (default: ${DEFAULT-VALUE}).", paramLabel = "<num>")
		private double defaultScore = ZaddSupplier.DEFAULT_SCORE;

		@Override
		protected ZaddSupplier collectionOperationBuilder() {
			ZaddSupplier supplier = new ZaddSupplier();
			supplier.setScoreField(scoreField);
			supplier.setDefaultScore(defaultScore);
			return supplier;
		}

	}

	protected abstract static class AbstractCollectionCommand extends OperationCommand {

		@Option(names = "--member-space", description = "Keyspace prefix for member IDs.", paramLabel = "<str>")
		private String memberSpace;

		@Option(arity = "1..*", names = { "-m",
				"--members" }, description = "Member field names for collections.", paramLabel = "<fields>")
		private List<String> memberFields;

		@Override
		protected AbstractMapOperationBuilder operationBuilder() {
			AbstractCollectionMapOperationBuilder builder = collectionOperationBuilder();
			builder.setMemberSpace(memberSpace);
			builder.setMemberFields(memberFields);
			return builder;
		}

		protected abstract AbstractCollectionMapOperationBuilder collectionOperationBuilder();

	}

	@Command(mixinStandardHelpOptions = true)
	protected abstract static class OperationCommand extends BaseCommand {

		@Option(names = { "-p", "--keyspace" }, description = "Keyspace prefix.", paramLabel = "<str>")
		private String keyspace;

		@Option(names = { "-k", "--keys" }, arity = "1..*", description = "Key fields.", paramLabel = "<fields>")
		private List<String> keys;

		@Option(names = { "-s",
				"--separator" }, description = "Key separator (default: ${DEFAULT-VALUE}).", paramLabel = "<str>")
		private String keySeparator = AbstractMapOperationBuilder.DEFAULT_SEPARATOR;

		@Option(names = { "-r", "--remove" }, description = "Remove key or member fields the first time they are used.")
		private boolean removeFields = AbstractMapOperationBuilder.DEFAULT_REMOVE_FIELDS;

		@Option(names = "--ignore-missing", description = "Ignore missing fields.")
		private boolean ignoreMissingFields = AbstractMapOperationBuilder.DEFAULT_IGNORE_MISSING_FIELDS;

		public Operation<String, String, Map<String, Object>, Object> operation() {
			AbstractMapOperationBuilder builder = operationBuilder();
			builder.setIgnoreMissingFields(ignoreMissingFields);
			builder.setKeyFields(keys);
			builder.setKeySeparator(keySeparator);
			builder.setKeyspace(keyspace);
			builder.setRemoveFields(removeFields);
			return builder.build();
		}

		protected abstract AbstractMapOperationBuilder operationBuilder();

	}

}
