package com.redislabs.riot.cli.in.file;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.separator.DefaultRecordSeparatorPolicy;
import org.springframework.batch.item.file.transform.FieldSet;

import lombok.Getter;
import picocli.CommandLine.Option;

public abstract class AbstractFlatFileImport extends AbstractFileImport {

	@Getter
	@Option(names = "--encoding", description = "Encoding for this input source. (default: ${DEFAULT-VALUE}).")
	private String encoding = FlatFileItemReader.DEFAULT_CHARSET;
	@Getter
	@Option(names = "--fields", arity = "1..*", description = "Names of the fields in the order they occur within the delimited file.")
	private String[] names = new String[0];
	@Getter
	@Option(names = "--skip", description = "Number of lines to skip at the beginning of reading the file. (default: ${DEFAULT-VALUE}).")
	private int linesToSkip = 0;

	protected FlatFileItemReaderBuilder<Map<String, Object>> builder() throws IOException {
		FlatFileItemReaderBuilder<Map<String, Object>> builder = new FlatFileItemReaderBuilder<Map<String, Object>>();
		builder.resource(resource());
		if (encoding != null) {
			builder.encoding(encoding);
		}
		builder.linesToSkip(getLinesToSkip());
		builder.strict(true);
		builder.saveState(false);
		builder.fieldSetMapper(new MapFieldSetMapper());
		builder.recordSeparatorPolicy(new DefaultRecordSeparatorPolicy());
		return builder;
	}

	private static class MapFieldSetMapper implements FieldSetMapper<Map<String, Object>> {

		@Override
		public Map<String, Object> mapFieldSet(FieldSet fieldSet) {
			Map<String, Object> fields = new HashMap<>();
			String[] names = fieldSet.getNames();
			for (int index = 0; index < names.length; index++) {
				String name = names[index];
				String value = fieldSet.readString(index);
				if (value == null || value.length() == 0) {
					continue;
				}
				fields.put(name, value);
			}
			return fields;
		}
	}

}
