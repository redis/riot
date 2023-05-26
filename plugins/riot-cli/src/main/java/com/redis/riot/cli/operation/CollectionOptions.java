package com.redis.riot.cli.operation;

import java.util.Optional;

import picocli.CommandLine.Option;

public class CollectionOptions {

	@Option(names = "--member-space", description = "Keyspace prefix for member IDs.", paramLabel = "<str>")
	private Optional<String> memberSpace = Optional.empty();

	@Option(arity = "1..*", names = { "-m",
			"--members" }, description = "Member field names for collections.", paramLabel = "<fields>")
	private String[] memberFields;

	public String[] getMemberFields() {
		return memberFields;
	}

	public Optional<String> getMemberSpace() {
		return memberSpace;
	}

	public void setMemberFields(String... memberFields) {
		this.memberFields = memberFields;
	}

	public void setMemberSpace(Optional<String> memberSpace) {
		this.memberSpace = memberSpace;
	}

}
