package com.redislabs.riot.redis;

import java.util.Map;

import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.StreamItemWriter;
import org.springframework.core.convert.converter.Converter;

import com.redislabs.riot.TransferContext;
import com.redislabs.riot.convert.MapFlattener;
import com.redislabs.riot.convert.ObjectToStringConverter;

import io.lettuce.core.XAddArgs;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "xadd", aliases = "x", description = "Append entries to streams")
public class XaddCommand extends AbstractKeyCommand {

	@Option(names = "--id", description = "Stream entry ID field", paramLabel = "<field>")
	private String idField;
	@Option(names = "--maxlen", description = "Stream maxlen", paramLabel = "<int>")
	private Long maxlen;
	@Option(names = "--trim", description = "Stream efficient trimming ('~' flag)")
	private boolean approximateTrimming;

	@Override
	public ItemWriter<Map<String, Object>> writer(TransferContext context) throws Exception {
		return configure(StreamItemWriter.<Map<String, Object>>builder(context.getClient())
				.poolConfig(context.getRedisOptions().poolConfig()).argsConverter(argsConverter())
				.bodyConverter(new MapFlattener<>(new ObjectToStringConverter()))).build();
	}

	private Converter<Map<String, Object>, XAddArgs> argsConverter() {
		if (idField == null) {
			XAddArgs args = xAddArgs();
			return s -> args;
		}
		Converter<Map<String, Object>, String> idExtractor = stringFieldExtractor(idField);
		return s -> xAddArgs().id(idExtractor.convert(s));
	}

	private XAddArgs xAddArgs() {
		XAddArgs args = new XAddArgs();
		if (maxlen != null) {
			args.maxlen(maxlen);
		}
		args.approximateTrimming(approximateTrimming);
		return args;
	}

}
