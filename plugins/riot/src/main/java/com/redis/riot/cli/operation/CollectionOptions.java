package com.redis.riot.cli.operation;

import picocli.CommandLine.Option;

public class CollectionOptions {

	@Option(names = "--member-space", description = "Keyspace prefix for member IDs.", paramLabel = "<str>")
	private String memberSpace;
	@Option(arity = "1..*", names = { "-m",
			"--members" }, description = "Member field names for collections.", paramLabel = "<fields>")
	private String[] memberFields;

	public String[] getMemberFields() {
		return memberFields;
	}

	public String getMemberSpace() {
		return memberSpace;
	}

	public void setMemberFields(String... memberFields) {
		this.memberFields = memberFields;
	}

	public void setMemberSpace(String memberSpace) {
		this.memberSpace = memberSpace;
	}

}
