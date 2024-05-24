package com.redis.riot.operation;

import java.util.Map;
import java.util.function.Function;

import picocli.CommandLine.ArgGroup;

abstract class AbstractMemberOperationCommand extends AbstractOperationCommand {

	@ArgGroup(exclusive = false)
	private MemberOperationArgs memberArgs = new MemberOperationArgs();

	protected Function<Map<String, Object>, String> memberFunction() {
		return idFunction(memberArgs.getMemberSpace(), memberArgs.getMemberFields());
	}

}