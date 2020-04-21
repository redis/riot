package com.redislabs.riot.cli.file;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public enum FileType {
	CSV("csv"), TSV("tsv"), FIXED("fw"), JSON("json"), XML("xml");

	private final String extension;

	FileType(String extension) {
		this.extension = extension;
	}

	public String getExtension() {
		return extension;
	}
}
