package com.redislabs.riot;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

import lombok.Getter;
import picocli.CommandLine;

@SuppressWarnings({ "rawtypes", "unchecked" })
public abstract class AbstractImportCommand extends AbstractTransferCommand<Object, Object> {

	public enum RedisCommand {
		EVALSHA, EXPIRE, GEOADD, HMSET, LPUSH, NOOP, RPUSH, SADD, SET, XADD, ZADD
	}

	public enum SourceType {
		STRING_MAP, OBJECT_MAP, KEY_VALUE
	}

	@CommandLine.Option(arity = "1..*", names = "--spel", description = "SpEL expression to produce a field", paramLabel = "<field=exp>")
	private Map<String, String> spel = new HashMap<>();
	@CommandLine.Option(arity = "1..*", names = "--spel-var", description = "Register a variable in the SpEL processor context", paramLabel = "<v=exp>")
	private Map<String, String> variables = new HashMap<>();
	@CommandLine.Option(arity = "1..*", names = "--regex", description = "Extract named values from source field using regex", paramLabel = "<field=exp>")
	private Map<String, String> regexes = new HashMap<>();
	@CommandLine.Option(names = "--date-format", description = "Processor date format (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
	private String dateFormat = new SimpleDateFormat().toPattern();
	@CommandLine.Option(names = "--remove-fields", description = "Remove fields already being used (e.g. keys or member ids)")
	private boolean removeFields;
	@Getter
	@CommandLine.Option(names = "--command", description = "Redis command: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<name>")
	private RedisCommand redisCommand = RedisCommand.HMSET;
	@CommandLine.Option(names = "--key-separator", description = "Key separator (default: ${DEFAULT-VALUE})", paramLabel = "<str>")
	private String keySeparator = KeyMaker.DEFAULT_SEPARATOR;
	@CommandLine.Option(names = { "-p", "--keyspace" }, description = "Keyspace prefix", paramLabel = "<str>")
	private String keyspace;
	@CommandLine.Option(names = { "-k", "--keys" }, arity = "1..*", description = "Key fields", paramLabel = "<fields>")
	private String[] keys = new String[0];
	@CommandLine.ArgGroup(exclusive = false, heading = "Redis command options%n")
	private final RedisImportOptions redisImportOptions = new RedisImportOptions();

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

	private KeyMaker keyMaker() {
		return idMaker(keyspace, keys);
	}

	private KeyMaker memberIdMaker() {
		return idMaker(redisImportOptions.getMemberSpace(), redisImportOptions.getMemberFields());
	}

	private KeyMaker idMaker(String prefix, String[] fields) {
		Converter[] extractors = new Converter[fields.length];
		for (int index = 0; index < fields.length; index++) {
			extractors[index] = FieldExtractor.builder().remove(removeFields).field(fields[index]).build();
		}
		return KeyMaker.builder().separator(keySeparator).prefix(prefix).extractors(extractors).build();
	}

	protected ItemWriter writer(SourceType sourceType) {
		switch (sourceType) {
		case KEY_VALUE:
			return configure(RedisKeyValueItemWriter.builder()).build();
		case OBJECT_MAP:
		case STRING_MAP:
			switch (redisCommand) {
			case EVALSHA:
				return configure(Eval.<Map<String, Object>>builder().sha(redisImportOptions.getEvalSha())
						.outputType(redisImportOptions.getEvalOutputType())
						.keysConverter(ObjectMapToStringArrayConverter.builder().fields(keys).build())
						.argsConverter(ObjectMapToStringArrayConverter.builder()
								.fields(redisImportOptions.getEvalArgs()).build())).build();
			case EXPIRE:
				return configureKeyCommandWriterBuilder(
						Expire.<Map<String, Object>>builder()
								.timeoutConverter(numberFieldExtractor(sourceType, Long.class,
										redisImportOptions.getTimeoutField(), redisImportOptions.getTimeoutDefault())))
												.build();
			case HMSET:
				return configureKeyCommandWriterBuilder(
						Hmset.<Map<String, Object>>builder().mapConverter(JsonMapFlattener::flattenToStringMap))
								.build();
			case GEOADD:
				return configureCollectionCommandWriterBuilder(Geoadd.<Map<String, Object>>builder()
						.longitudeConverter(doubleFieldExtractor(sourceType, redisImportOptions.getLongitudeField()))
						.latitudeConverter(doubleFieldExtractor(sourceType, redisImportOptions.getLatitudeField())))
								.build();
			case LPUSH:
				return configureCollectionCommandWriterBuilder(Lpush.<Map<String, Object>>builder()).build();
			case RPUSH:
				return configureCollectionCommandWriterBuilder(Rpush.<Map<String, Object>>builder()).build();
			case SADD:
				return configureCollectionCommandWriterBuilder(Sadd.<Map<String, Object>>builder()).build();
			case SET:
				return configureKeyCommandWriterBuilder(
						Set.<Map<String, Object>>builder().valueConverter(stringValueConverter(sourceType))).build();
			case NOOP:
				return configure(Noop.<Map<String, Object>>builder()).build();
			case XADD:
				return configureKeyCommandWriterBuilder(
						Xadd.<Map<String, Object>>builder().bodyConverter(JsonMapFlattener::flattenToStringMap)
								.idConverter(stringFieldExtractor(sourceType, redisImportOptions.getXaddIdField()))
								.maxlen(redisImportOptions.getXaddMaxlen())
								.approximateTrimming(redisImportOptions.isXaddTrim())).build();
			case ZADD:
				return configureCollectionCommandWriterBuilder(
						Zadd.<Map<String, Object>>builder()
								.scoreConverter(numberFieldExtractor(sourceType, Double.class,
										redisImportOptions.getScoreField(), redisImportOptions.getScoreDefault())))
												.build();
			}
			throw new IllegalArgumentException("Command not supported: " + redisCommand);
		}
		throw new IllegalArgumentException("Source type not supported: " + sourceType);
	}

	private Converter<Map<String, Object>, Double> doubleFieldExtractor(SourceType sourceType, String field) {
		return numberFieldExtractor(sourceType, Double.class, field, null);
	}

	private Converter<Map<String, Object>, Object> fieldExtractor(String field, Object defaultValue) {
		return FieldExtractor.builder().field(field).remove(removeFields).defaultValue(defaultValue).build();
	}

	private Converter<Map<String, Object>, String> stringFieldExtractor(SourceType sourceType, String field) {
		Converter<Map<String, Object>, Object> extractor = fieldExtractor(field, null);
		if (extractor == null) {
			return null;
		}
		if (sourceType == SourceType.STRING_MAP) {
			return (Converter) extractor;
		}
		return new CompositeConverter(extractor, ConverterFactory.getObjectToStringConverter());
	}

	private <T extends Number> Converter<Map<String, Object>, T> numberFieldExtractor(SourceType sourceType,
			Class<T> targetType, String field, T defaultValue) {
		Converter<Map<String, Object>, Object> extractor = fieldExtractor(field, defaultValue);
		if (sourceType == SourceType.STRING_MAP) {
			return new CompositeConverter(extractor, ConverterFactory.getStringToNumberConverter(targetType));
		}
		return new CompositeConverter(extractor, new ObjectToNumberConverter<>(targetType));
	}

	private <B extends KeyCommandWriterBuilder<B, Map<String, Object>>> B configureKeyCommandWriterBuilder(B builder) {
		return configure(builder.keyConverter(keyMaker()));
	}

	private <B extends CollectionCommandWriterBuilder<B, Map<String, Object>>> B configureCollectionCommandWriterBuilder(
			B builder) {
		return configureKeyCommandWriterBuilder(builder.keyConverter(keyMaker()).memberIdConverter(memberIdMaker()));
	}

	private Converter<Map<String, Object>, String> stringValueConverter(SourceType sourceType) {
		switch (redisImportOptions.getStringFormat()) {
		case RAW:
			return stringFieldExtractor(sourceType, redisImportOptions.getStringField());
		case XML:
			return new ObjectMapperConverter<>(new XmlMapper().writer().withRootName(redisImportOptions.getXmlRoot()));
		case JSON:
			return new ObjectMapperConverter<>(jsonMapper().writer().withRootName(redisImportOptions.getXmlRoot()));
		}
		throw new IllegalArgumentException("Unsupported String format: " + redisImportOptions.getStringFormat());
	}

	private ObjectMapper jsonMapper() {
		ObjectMapper mapper = new ObjectMapper();
		mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
		mapper.setSerializationInclusion(Include.NON_NULL);
		return mapper;
	}

}
