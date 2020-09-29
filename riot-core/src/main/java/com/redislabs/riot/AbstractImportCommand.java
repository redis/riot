package com.redislabs.riot;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.RedisKeyValueItemWriter;
import org.springframework.batch.item.redis.support.CommandItemWriters.CollectionCommandWriterBuilder;
import org.springframework.batch.item.redis.support.CommandItemWriters.Eval;
import org.springframework.batch.item.redis.support.CommandItemWriters.Expire;
import org.springframework.batch.item.redis.support.CommandItemWriters.Geoadd;
import org.springframework.batch.item.redis.support.CommandItemWriters.Hmset;
import org.springframework.batch.item.redis.support.CommandItemWriters.KeyCommandWriterBuilder;
import org.springframework.batch.item.redis.support.CommandItemWriters.Lpush;
import org.springframework.batch.item.redis.support.CommandItemWriters.Noop;
import org.springframework.batch.item.redis.support.CommandItemWriters.Rpush;
import org.springframework.batch.item.redis.support.CommandItemWriters.Sadd;
import org.springframework.batch.item.redis.support.CommandItemWriters.Set;
import org.springframework.batch.item.redis.support.CommandItemWriters.Xadd;
import org.springframework.batch.item.redis.support.CommandItemWriters.Zadd;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.batch.item.support.PassThroughItemProcessor;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.support.ConverterFactory;
import org.springframework.vault.support.JsonMapFlattener;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redislabs.riot.convert.CompositeConverter;
import com.redislabs.riot.convert.FieldExtractor;
import com.redislabs.riot.convert.KeyMaker;
import com.redislabs.riot.convert.ObjectMapToStringArrayConverter;
import com.redislabs.riot.convert.ObjectMapperConverter;
import com.redislabs.riot.convert.ObjectToNumberConverter;
import com.redislabs.riot.processor.MapProcessor;
import com.redislabs.riot.processor.SpelProcessor;

import io.lettuce.core.ScriptOutputType;
import lombok.Getter;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;

@SuppressWarnings({ "rawtypes", "unchecked" })
public abstract class AbstractImportCommand extends AbstractTransferCommand<Object, Object> {

	public enum RedisCommand {
		EVALSHA, EXPIRE, GEOADD, HMSET, LPUSH, NOOP, RPUSH, SADD, SET, XADD, ZADD
	}

	public enum SourceType {
		STRING_MAP, OBJECT_MAP, KEY_VALUE
	}

	public enum StringFormat {
		RAW, XML, JSON
	}

	@CommandLine.Option(arity = "1..*", names = "--spel", description = "SpEL expression to produce a field", paramLabel = "<field=exp>")
	private Map<String, String> spel = new HashMap<>();
	@CommandLine.Option(arity = "1..*", names = "--spel-var", description = "Register a variable in the SpEL processor context", paramLabel = "<v=exp>")
	private Map<String, String> variables = new HashMap<>();
	@CommandLine.Option(arity = "1..*", names = "--regex", description = "Extract named values from source field using regex", paramLabel = "<field=exp>")
	private Map<String, String> regexes = new HashMap<>();
	@CommandLine.Option(names = "--date-format", description = "Processor date format (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
	private String dateFormat = new SimpleDateFormat().toPattern();

	@ArgGroup(exclusive = false, multiplicity = "0..*", heading = "Redis command options%n")
	private List<RedisCommandComposite> commands;

	static class RedisCommandComposite {

		@CommandLine.Option(names = { "-c",
				"--command" }, defaultValue = "HMSET", description = "${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<name>")
		private RedisCommand command;

		@ArgGroup(exclusive = false, multiplicity = "0..1")
		RedisCommandOptions options;
	}

	@Getter
	static class RedisCommandOptions {

		@CommandLine.Option(names = "--remove-fields", description = "Remove fields already being used (e.g. keys or member ids)")
		private boolean removeFields;
		@CommandLine.Option(names = "--key-separator", defaultValue = ":", description = "Key separator (default: ${DEFAULT-VALUE})", paramLabel = "<str>")
		private String keySeparator;
		@CommandLine.Option(names = { "-p", "--keyspace" }, description = "Keyspace prefix", paramLabel = "<str>")
		private String keyspace;
		@CommandLine.Option(names = { "-k",
				"--keys" }, arity = "1..*", description = "Key fields", paramLabel = "<fields>")
		private String[] keys = new String[0];
		@CommandLine.Option(names = "--member-space", description = "Prefix for member IDs", paramLabel = "<str>")
		private String memberSpace;
		@CommandLine.Option(names = "--members", arity = "1..*", description = "Member field names for collections", paramLabel = "<fields>")
		private String[] memberFields = new String[0];
		@CommandLine.Option(names = "--eval-sha", description = "Digest", paramLabel = "<sha>")
		private String evalSha;
		@CommandLine.Option(names = "--eval-args", arity = "1..*", description = "EVAL arg field names", paramLabel = "<names>")
		private String[] evalArgs = new String[0];
		@CommandLine.Option(names = "--eval-output", defaultValue = "STATUS", description = "Output: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<type>")
		private ScriptOutputType evalOutputType;
		@CommandLine.Option(names = "--geo-lon", description = "Longitude field", paramLabel = "<field>")
		private String longitudeField;
		@CommandLine.Option(names = "--geo-lat", description = "Latitude field", paramLabel = "<field>")
		private String latitudeField;
		@CommandLine.Option(names = "--zset-score", description = "Name of the field to use for scores", paramLabel = "<field>")
		private String scoreField;
		@CommandLine.Option(names = "--zset-default", defaultValue = "1", description = "Score when field not present (default: ${DEFAULT-VALUE})", paramLabel = "<num>")
		private double scoreDefault;
		@CommandLine.Option(names = "--string-format", defaultValue = "JSON", description = "Serialization: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<fmt>")
		private StringFormat stringFormat;
		@CommandLine.Option(names = "--string-field", description = "String value field", paramLabel = "<field>")
		private String stringField;
		@CommandLine.Option(names = "--string-root", description = "XML root element name", paramLabel = "<name>")
		private String xmlRoot;
		@CommandLine.Option(names = "--expire-ttl", description = "EXPIRE timeout field", paramLabel = "<field>")
		private String timeoutField;
		@CommandLine.Option(names = "--expire-default", defaultValue = "60", description = "EXPIRE default timeout (default: ${DEFAULT-VALUE})", paramLabel = "<sec>")
		private long timeoutDefault;
		@CommandLine.Option(names = "--xadd-id", description = "Stream entry ID field", paramLabel = "<field>")
		private String xaddIdField;
		@CommandLine.Option(names = "--xadd-maxlen", description = "Stream maxlen", paramLabel = "<int>")
		private Long xaddMaxlen;
		@CommandLine.Option(names = "--xadd-trim", description = "Stream efficient trimming ('~' flag)")
		private boolean xaddTrim;
	}

	protected abstract ItemProcessor processor();

	protected ItemProcessor<Map<String, Object>, Map<String, Object>> mapProcessor() {
		List<ItemProcessor> processors = new ArrayList<>();
		if (!spel.isEmpty()) {
			processors.add(configure(SpelProcessor.builder().dateFormat(new SimpleDateFormat(dateFormat))
					.variables(variables).fields(spel)).build());
		}
		if (!regexes.isEmpty()) {
			processors.add(MapProcessor.builder().regexes(regexes).build());
		}
		if (processors.isEmpty()) {
			return new PassThroughItemProcessor();
		}
		if (processors.size() == 1) {
			return processors.get(0);
		}
		CompositeItemProcessor compositeItemProcessor = new CompositeItemProcessor();
		compositeItemProcessor.setDelegates(processors);
		return compositeItemProcessor;
	}

	protected Transfer<Object, Object> transfer(String name, ItemReader reader, SourceType sourceType) {
		return transfer(name, reader, processor(), writer(sourceType));
	}

	private ItemWriter writer(SourceType sourceType) {
		if (sourceType == SourceType.KEY_VALUE) {
			return configure(RedisKeyValueItemWriter.builder()).build();
		}
		if (commands == null || commands.isEmpty()) {
			throw new IllegalArgumentException("No Redis command specified");
		}
		if (commands.size() == 1) {
			return writer(sourceType, commands.get(0));
		}
		List<ItemWriter> writers = commands.stream().map(c -> writer(sourceType, c)).collect(Collectors.toList());
		CompositeItemWriter writer = new CompositeItemWriter();
		writer.setDelegates(writers);
		return writer;
	}

	private KeyMaker keyMaker(RedisCommandOptions options) {
		return idMaker(options.getKeyspace(), options.getKeySeparator(), options.getKeys(), options.isRemoveFields());
	}

	private KeyMaker memberIdMaker(RedisCommandOptions options) {
		return idMaker(options.getMemberSpace(), options.getKeySeparator(), options.getMemberFields(),
				options.isRemoveFields());
	}

	private KeyMaker idMaker(String prefix, String separator, String[] fields, boolean remove) {
		Converter[] extractors = new Converter[fields.length];
		for (int index = 0; index < fields.length; index++) {
			extractors[index] = FieldExtractor.builder().remove(remove).field(fields[index]).build();
		}
		return KeyMaker.builder().separator(separator).prefix(prefix).extractors(extractors).build();
	}

	private ItemWriter writer(SourceType sourceType, RedisCommandComposite composite) {
		RedisCommandOptions options = composite.options;
		switch (composite.command) {
		case EVALSHA:
			return configure(Eval.<Map<String, Object>>builder().sha(options.getEvalSha())
					.outputType(options.getEvalOutputType())
					.keysConverter(ObjectMapToStringArrayConverter.builder().fields(options.getKeys()).build())
					.argsConverter(ObjectMapToStringArrayConverter.builder().fields(options.getEvalArgs()).build()))
							.build();
		case EXPIRE:
			return configureKeyCommandWriterBuilder(
					Expire.<Map<String, Object>>builder().timeoutConverter(numberFieldExtractor(sourceType, Long.class,
							options.getTimeoutField(), options.isRemoveFields(), options.getTimeoutDefault())),
					options).build();
		case HMSET:
			return configureKeyCommandWriterBuilder(
					Hmset.<Map<String, Object>>builder().mapConverter(JsonMapFlattener::flattenToStringMap), options)
							.build();
		case GEOADD:
			return configureCollectionCommandWriterBuilder(Geoadd.<Map<String, Object>>builder()
					.longitudeConverter(
							doubleFieldExtractor(sourceType, options.getLongitudeField(), options.isRemoveFields()))
					.latitudeConverter(
							doubleFieldExtractor(sourceType, options.getLatitudeField(), options.isRemoveFields())),
					options).build();
		case LPUSH:
			return configureCollectionCommandWriterBuilder(Lpush.<Map<String, Object>>builder(), options).build();
		case RPUSH:
			return configureCollectionCommandWriterBuilder(Rpush.<Map<String, Object>>builder(), options).build();
		case SADD:
			return configureCollectionCommandWriterBuilder(Sadd.<Map<String, Object>>builder(), options).build();
		case SET:
			return configureKeyCommandWriterBuilder(
					Set.<Map<String, Object>>builder().valueConverter(stringValueConverter(sourceType, options)),
					options).build();
		case NOOP:
			return configure(Noop.<Map<String, Object>>builder()).build();
		case XADD:
			return configureKeyCommandWriterBuilder(Xadd.<Map<String, Object>>builder()
					.bodyConverter(JsonMapFlattener::flattenToStringMap)
					.idConverter(stringFieldExtractor(sourceType, options.getXaddIdField(), options.isRemoveFields()))
					.maxlen(options.getXaddMaxlen()).approximateTrimming(options.isXaddTrim()), options).build();
		case ZADD:
			return configureCollectionCommandWriterBuilder(
					Zadd.<Map<String, Object>>builder().scoreConverter(numberFieldExtractor(sourceType, Double.class,
							options.getScoreField(), options.isRemoveFields(), options.getScoreDefault())),
					options).build();
		}
		throw new IllegalArgumentException("Command not supported: " + composite.command);
	}

	private Converter<Map<String, Object>, Double> doubleFieldExtractor(SourceType sourceType, String field,
			boolean remove) {
		return numberFieldExtractor(sourceType, Double.class, field, remove, null);
	}

	private Converter<Map<String, Object>, Object> fieldExtractor(String field, boolean remove, Object defaultValue) {
		return FieldExtractor.builder().field(field).remove(remove).defaultValue(defaultValue).build();
	}

	private Converter<Map<String, Object>, String> stringFieldExtractor(SourceType sourceType, String field,
			boolean remove) {
		Converter<Map<String, Object>, Object> extractor = fieldExtractor(field, remove, null);
		if (extractor == null) {
			return null;
		}
		if (sourceType == SourceType.STRING_MAP) {
			return (Converter) extractor;
		}
		return new CompositeConverter(extractor, ConverterFactory.getObjectToStringConverter());
	}

	private <T extends Number> Converter<Map<String, Object>, T> numberFieldExtractor(SourceType sourceType,
			Class<T> targetType, String field, boolean remove, T defaultValue) {
		Converter<Map<String, Object>, Object> extractor = fieldExtractor(field, remove, defaultValue);
		if (sourceType == SourceType.STRING_MAP) {
			return new CompositeConverter(extractor, ConverterFactory.getStringToNumberConverter(targetType));
		}
		return new CompositeConverter(extractor, new ObjectToNumberConverter<>(targetType));
	}

	private <B extends KeyCommandWriterBuilder<B, Map<String, Object>>> B configureKeyCommandWriterBuilder(B builder,
			RedisCommandOptions options) {
		return configure(builder.keyConverter(keyMaker(options)));
	}

	private <B extends CollectionCommandWriterBuilder<B, Map<String, Object>>> B configureCollectionCommandWriterBuilder(
			B builder, RedisCommandOptions options) {
		return configureKeyCommandWriterBuilder(
				builder.keyConverter(keyMaker(options)).memberIdConverter(memberIdMaker(options)), options);
	}

	private Converter<Map<String, Object>, String> stringValueConverter(SourceType sourceType,
			RedisCommandOptions options) {
		switch (options.getStringFormat()) {
		case RAW:
			return stringFieldExtractor(sourceType, options.getStringField(), options.isRemoveFields());
		case XML:
			return new ObjectMapperConverter<>(new XmlMapper().writer().withRootName(options.getXmlRoot()));
		case JSON:
			return new ObjectMapperConverter<>(jsonMapper().writer().withRootName(options.getXmlRoot()));
		}
		throw new IllegalArgumentException("Unsupported String format: " + options.getStringFormat());
	}

	private ObjectMapper jsonMapper() {
		ObjectMapper mapper = new ObjectMapper();
		mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
		mapper.setSerializationInclusion(Include.NON_NULL);
		return mapper;
	}

}
