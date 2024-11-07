package com.redis.riot.file;

public class ResourceOptions {

	private boolean gzipped;
	private AwsOptions awsOptions = new AwsOptions();
	private GcpOptions gcpOptions = new GcpOptions();

	public AwsOptions getAwsOptions() {
		return awsOptions;
	}

	public void setAwsOptions(AwsOptions awsOptions) {
		this.awsOptions = awsOptions;
	}

	public GcpOptions getGcpOptions() {
		return gcpOptions;
	}

	public void setGcpOptions(GcpOptions gcpOptions) {
		this.gcpOptions = gcpOptions;
	}

	public boolean isGzipped() {
		return gzipped;
	}

	public void setGzipped(boolean gzipped) {
		this.gzipped = gzipped;
	}

}
