package com.redislabs.riot.cli.file;

import java.io.IOException;
import java.io.Writer;
import java.util.Map;

import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.FieldExtractor;

import picocli.CommandLine.Option;

public abstract class AbstractFlatFileWriterCommand extends AbstractFileWriterCommand {

	@Option(names = "--header", description = "Write field names on first line.")
	private boolean header = false;
	@Option(names = "--names", required = true, description = "Names of the fields to write on the first line.")
	private String[] names;
	@Option(names = "--line-separator", description = "String used to separate lines in output.")
	private String lineSeparator = FlatFileItemWriter.DEFAULT_LINE_SEPARATOR;

	@Override
	protected FlatFileItemWriter<Map<String, Object>> writer() {
		FlatFileItemWriterBuilder<Map<String, Object>> builder = new FlatFileItemWriterBuilder<>();
		builder.append(isAppend());
		builder.encoding(getEncoding());
		builder.forceSync(isForceSync());
		builder.lineSeparator(lineSeparator);
		builder.resource(resource());
		builder.saveState(false);
		if (header) {
			builder.headerCallback(new FlatFileHeaderCallback() {

				@Override
				public void writeHeader(Writer writer) throws IOException {
					writer.write(header(names));
				}
			});
		}
		configure(builder);
		return builder.build();
	}

	protected FieldExtractor<Map<String, Object>> fieldExtractor() {
		return new MapFieldExtractor(names);
	}

	protected abstract String header(String[] names);

	protected abstract void configure(FlatFileItemWriterBuilder<Map<String, Object>> builder);

	private static class MapFieldExtractor implements FieldExtractor<Map<String, Object>> {

		private String[] names;

		public MapFieldExtractor(String[] names) {
			this.names = names;
		}

		@Override
		public Object[] extract(Map<String, Object> item) {
			Object[] fields = new Object[names.length];
			for (int index = 0; index < names.length; index++) {
				fields[index] = item.get(names[index]);
			}
			return fields;
		}

	}

}
