package com.redis.riot.cli;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import com.redis.riot.core.FileUtils;

import picocli.CommandLine.Parameters;

public class FileImportOptions extends FileOptions {

	@Parameters(arity = "0..*", description = "One ore more files or URLs", paramLabel = "FILE")
	protected List<String> files = new ArrayList<>();

	public FileImportOptions() {
	}

	protected FileImportOptions(Builder<?> builder) {
		super(builder);
	}

	public List<String> getFiles() {
		return files;
	}

	public void setFiles(List<String> files) {
		this.files = files;
	}

	public Stream<Resource> getResources() {
		return resources(files);
	}

	public Stream<Resource> resources(Iterable<String> files) {
		Assert.isTrue(!ObjectUtils.isEmpty(files), "No file specified");
		return StreamSupport.stream(files.spliterator(), false).flatMap(FileUtils::expand).map(this::inputResource);
	}

}
