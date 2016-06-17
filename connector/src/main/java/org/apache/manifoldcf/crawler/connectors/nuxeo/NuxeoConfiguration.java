package org.apache.manifoldcf.crawler.connectors.nuxeo;

public class NuxeoConfiguration {

	public static interface Server{
		
		public static final String USERNAME = "username";
		public static final String PASSWORD = "password";
		public static final String PROTOCOL = "protocol";
		public static final String HOST = "host";
		public static final String PORT = "port";
		public static final String PATH = "path";
		
		public static final String PROTOCOL_DEFAULT_VALUE = "http";
		public static final String HOST_DEFAULT_VALUE = "";
		public static final String PORT_DEFAULT_VALUE = "8080";
		public static final String PATH_DEFAULT_VALUE = "/nuxeo";
		public static final String USERNAME_DEFAULT_VALUE = "";
		public static final String PASSWORD_DEFAULT_VALUE = "";
		
	}
}
