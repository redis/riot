package com.redis.riot.file;

import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import org.springframework.util.Assert;

public class FileType {

	public static final String EXTENSION_CSV = "csv";
	public static final String EXTENSION_TSV = "tsv";
	public static final String EXTENSION_PSV = "psv";
	public static final String EXTENSION_XML = "xml";
	public static final String EXTENSION_JSON = "json";
	public static final String EXTENSION_JSONL = "jsonl";
	public static final String EXTENSION_FW = "fw";

	public static final FileType DELIMITED = new FileType("Delimited", EXTENSION_CSV, EXTENSION_TSV, EXTENSION_PSV);
	public static final FileType FIXED_WIDTH = new FileType("Fixed-Width", EXTENSION_FW);
	public static final FileType JSON = new FileType("JSON", EXTENSION_JSON);
	public static final FileType JSONL = new FileType("JSON Lines", EXTENSION_JSONL);
	public static final FileType XML = new FileType("XML", EXTENSION_XML);

	private final String name;
	private final Set<String> extensions = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

	public FileType(String name, String... extensions) {
		Assert.notNull(name, "Name must not be null");
		this.name = name;
		this.extensions.addAll(Arrays.asList(extensions));
	}

	public String[] getExtensions() {
		return extensions.toArray(new String[0]);
	}

	public boolean supportsExtension(String extension) {
		return extensions.contains(extension);
	}

	public String getName() {
		return name;
	}

	@Override
	public int hashCode() {
		return Objects.hash(name);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		FileType other = (FileType) obj;
		return Objects.equals(name, other.name);
	}
}
