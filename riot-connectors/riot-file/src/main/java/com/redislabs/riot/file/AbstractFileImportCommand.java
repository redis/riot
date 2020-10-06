package com.redislabs.riot.file;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.batch.item.xml.XmlItemReader;
import org.springframework.batch.item.xml.XmlObjectReader;
import org.springframework.batch.item.xml.builder.XmlItemReaderBuilder;
import org.springframework.core.io.Resource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redislabs.riot.AbstractImportCommand;

import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;

@Slf4j
@Command
public abstract class AbstractFileImportCommand<I, O> extends AbstractImportCommand<I, O> {

	@Parameters(arity = "1..*", description = "File path or URL", paramLabel = "FILE")
	private List<String> files = new ArrayList<>();
	@Mixin
	protected FileOptions fileOptions = new FileOptions();

	@Override
	protected List<ItemReader<I>> readers() throws IOException {
		List<String> fileList = new ArrayList<>();
		for (String file : files) {
			if (fileOptions.isFile(file)) {
				Path path = Paths.get(file);
				if (Files.exists(path)) {
					fileList.add(file);
				} else {
					// Path might be glob pattern
					Path parent = path.getParent();
					try (DirectoryStream<Path> stream = Files.newDirectoryStream(parent,
							path.getFileName().toString())) {
						stream.forEach(p -> fileList.add(p.toString()));
					} catch (IOException e) {
						log.debug("Could not list files in {}", path, e);
					}
				}
			} else {
				fileList.add(file);
			}
		}
		List<ItemReader<I>> readers = new ArrayList<>(fileList.size());
		for (String file : fileList) {
			FileType fileType = fileOptions.fileType(file);
			Resource resource = fileOptions.inputResource(file);
			AbstractItemStreamItemReader<I> reader = reader(file, fileType, resource);
			reader.setName(fileOptions.fileName(resource));
			readers.add(reader);
		}
		return readers;
	}

	protected abstract AbstractItemStreamItemReader<I> reader(String file, FileType fileType, Resource resource)
			throws IOException;

	protected <T> JsonItemReader<T> jsonReader(Resource resource, Class<T> clazz) {
		JsonItemReaderBuilder<T> jsonReaderBuilder = new JsonItemReaderBuilder<>();
		jsonReaderBuilder.name("json-file-reader");
		jsonReaderBuilder.resource(resource);
		JacksonJsonObjectReader<T> jsonObjectReader = new JacksonJsonObjectReader<>(clazz);
		jsonObjectReader.setMapper(new ObjectMapper());
		jsonReaderBuilder.jsonObjectReader(jsonObjectReader);
		return jsonReaderBuilder.build();
	}

	protected <T> XmlItemReader<T> xmlReader(Resource resource, Class<T> clazz) {
		XmlItemReaderBuilder<T> xmlReaderBuilder = new XmlItemReaderBuilder<>();
		xmlReaderBuilder.name("xml-file-reader");
		xmlReaderBuilder.resource(resource);
		XmlObjectReader<T> xmlObjectReader = new XmlObjectReader<>(clazz);
		xmlObjectReader.setMapper(new XmlMapper());
		xmlReaderBuilder.xmlObjectReader(xmlObjectReader);
		return xmlReaderBuilder.build();
	}

}
