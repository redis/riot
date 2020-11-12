package com.redislabs.riot.redis;

import java.util.Map;

import org.springframework.batch.item.redis.RedisStreamItemWriter;
import org.springframework.batch.item.redis.support.ConstantConverter;
import org.springframework.core.convert.converter.Converter;

import com.redislabs.riot.convert.MapFlattener;
import com.redislabs.riot.convert.ObjectToStringConverter;

import io.lettuce.core.XAddArgs;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "xadd")
public class XaddCommand extends AbstractKeyCommand {

    @Option(names = "--id", description = "Stream entry ID field", paramLabel = "<field>")
    private String idField;

    @Option(names = "--maxlen", description = "Stream maxlen", paramLabel = "<int>")
    private Long maxlen;

    @Option(names = "--trim", description = "Stream efficient trimming ('~' flag)")
    private boolean approximateTrimming;

    @Override
    public RedisStreamItemWriter<Map<String, Object>> writer() throws Exception {
	return configure(RedisStreamItemWriter.<Map<String, Object>>builder().argsConverter(argsConverter())
		.bodyConverter(new MapFlattener<>(new ObjectToStringConverter()))).build();
    }

    private Converter<Map<String, Object>, XAddArgs> argsConverter() {
	if (idField == null) {
	    return new ConstantConverter<>(xAddArgs());
	}
	return new XAddArgsConverter();
    }

    private XAddArgs xAddArgs() {
	XAddArgs args = new XAddArgs();
	if (maxlen != null) {
	    args.maxlen(maxlen);
	}
	args.approximateTrimming(approximateTrimming);
	return args;
    }

    class XAddArgsConverter implements Converter<Map<String, Object>, XAddArgs> {

	private final Converter<Map<String, Object>, String> idExtractor = stringFieldExtractor(idField);

	@Override
	public XAddArgs convert(Map<String, Object> source) {
	    return xAddArgs().id(idExtractor.convert(source));
	}

    }

}
