package com.redis.riot;

import com.redis.riot.file.ResourceOptions;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public class ResourceArgs {

	@Option(names = { "-z", "--gzip" }, description = "File is gzip compressed.")
	private boolean gzipped;

	@ArgGroup(exclusive = false)
	private AwsArgs awsArgs = new AwsArgs();

	@ArgGroup(exclusive = false)
	private GcpArgs gcpArgs = new GcpArgs();

	public ResourceOptions resourceOptions() {
		ResourceOptions options = new ResourceOptions();
		options.setAwsOptions(awsArgs.awsOptions());
		options.setGcpOptions(gcpArgs.gcpOptions());
		options.setGzipped(gzipped);
		return options;
	}

	public boolean isGzipped() {
		return gzipped;
	}

	public void setGzipped(boolean gzipped) {
		this.gzipped = gzipped;
	}

	public AwsArgs getAwsArgs() {
		return awsArgs;
	}

	public void setAwsArgs(AwsArgs awsArgs) {
		this.awsArgs = awsArgs;
	}

	public GcpArgs getGcpArgs() {
		return gcpArgs;
	}

	public void setGcpArgs(GcpArgs gcpArgs) {
		this.gcpArgs = gcpArgs;
	}

}
