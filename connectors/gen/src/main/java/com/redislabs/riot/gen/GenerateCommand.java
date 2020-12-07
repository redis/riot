package com.redislabs.riot.gen;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.springframework.batch.item.redis.support.Transfer;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.RediSearchCommands;
import com.redislabs.lettusearch.RediSearchUtils;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.index.IndexInfo;
import com.redislabs.lettusearch.index.field.Field;
import com.redislabs.lettusearch.index.field.GeoField;
import com.redislabs.lettusearch.index.field.TagField;
import com.redislabs.lettusearch.index.field.TextField;
import com.redislabs.riot.AbstractMapImportCommand;
import com.redislabs.riot.TransferContext;

import io.lettuce.core.RedisURI;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "import", aliases = { "i" }, description = "Import generated data")
public class GenerateCommand extends AbstractMapImportCommand<Map<String, Object>, Map<String, Object>> {

	@Parameters(description = "SpEL expressions", paramLabel = "SPEL")
	private Map<String, String> fakerFields = new LinkedHashMap<>();
	@Option(names = "--introspect", description = "Use given search index to introspect Faker fields", paramLabel = "<index>")
	private String fakerIndex;
	@Option(names = "--locale", description = "Faker locale (default: ${DEFAULT-VALUE})", paramLabel = "<tag>")
	private Locale locale = Locale.ENGLISH;
	@Option(names = "--metadata", description = "Include metadata (index, partition)")
	private boolean includeMetadata;
	@Option(names = "--start", description = "Start index (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private long start = 0;
	@Option(names = "--end", description = "End index (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private long end = 1000;
	@Option(names = "--sleep", description = "Duration in ms to sleep before each item generation (default: ${DEFAULT-VALUE})", paramLabel = "<ms>")
	private long sleep = 0;

	@Override
	protected List<Transfer<Map<String, Object>, Map<String, Object>>> transfers(TransferContext context)
			throws Exception {
		FakerItemReader reader = FakerItemReader.builder().locale(locale).includeMetadata(includeMetadata)
				.fields(fakerFields(context.getRedisOptions().uri())).start(start).end(end).sleep(sleep).build();
		long count = end - start;
		reader.setMaxItemCount(Math.toIntExact(count));
		return Collections.singletonList(transfer(reader, mapProcessor(context.getClient()), writer(context)));
	}

	private String expression(Field<String> field) {
		if (field instanceof TextField) {
			return "lorem.paragraph";
		}
		if (field instanceof TagField) {
			return "number.digits(10)";
		}
		if (field instanceof GeoField) {
			return "address.longitude.concat(',').concat(address.latitude)";
		}
		return "number.randomDouble(3,-1000,1000)";
	}

	private Map<String, String> fakerFields(RedisURI uri) {
		Map<String, String> fields = new LinkedHashMap<>(fakerFields);
		if (fakerIndex == null) {
			return fields;
		}
		RediSearchClient client = RediSearchClient.create(uri);
		StatefulRediSearchConnection<String, String> connection = client.connect();
		RediSearchCommands<String, String> commands = connection.sync();
		IndexInfo<String> info = RediSearchUtils.getInfo(commands.ftInfo(fakerIndex));
		for (Field<String> field : info.getFields()) {
			fields.put(field.getName(), expression(field));
		}
		return fields;
	}

}
