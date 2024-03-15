package com.redis.riot.cli;

public class ManifestVersionProvider extends AbstractManifestVersionProvider {

	private static final String IMPLEMENTATION_TITLE = "riot";

	@Override
	protected String getImplementationTitle() {
		return IMPLEMENTATION_TITLE;
	}

	@Override
	public String[] getVersion() throws Exception {
		return new String[] {
				// @formatter:off
				"",
                "      ▀        █     @|fg(4;1;1) ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄|@",
                " █ ██ █  ███  ████   @|fg(4;2;1) ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄|@",
                " ██   █ █   █  █     @|fg(5;4;1) ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄|@",
                " █    █ █   █  █     @|fg(1;4;1) ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄|@",
                " █    █  ███    ██   @|fg(0;3;4) ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄|@" + "  v" + getVersionString(),
                "" 
                // @formatter:on
		};
	}

}
