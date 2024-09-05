package com.redis.riot.operation;

import java.util.List;

import picocli.CommandLine.Option;

public class MemberOperationArgs {

	@Option(names = "--member-space", description = "Keyspace prefix for member IDs.", paramLabel = "<str>")
	private String memberSpace;

	@Option(arity = "1..*", names = "--member", description = "Member field names for collections.", paramLabel = "<fields>")
	private List<String> memberFields;

	public String getMemberSpace() {
		return memberSpace;
	}

	public void setMemberSpace(String space) {
		this.memberSpace = space;
	}

	public List<String> getMemberFields() {
		return memberFields;
	}

	public void setMemberFields(List<String> fields) {
		this.memberFields = fields;
	}

	@Override
	public String toString() {
		return "MemberOperationArgs [memberSpace=" + memberSpace + ", memberFields=" + memberFields + "]";
	}

}
