package org.apache.manifoldcf.crawler.connectors.nuxeo;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.manifoldcf.agents.interfaces.ServiceInterruption;
import org.apache.manifoldcf.core.interfaces.ConfigParams;
import org.apache.manifoldcf.core.interfaces.IHTTPOutput;
import org.apache.manifoldcf.core.interfaces.IPasswordMapperActivity;
import org.apache.manifoldcf.core.interfaces.IPostParameters;
import org.apache.manifoldcf.core.interfaces.IThreadContext;
import org.apache.manifoldcf.core.interfaces.ManifoldCFException;
import org.apache.manifoldcf.core.interfaces.Specification;
import org.apache.manifoldcf.crawler.connectors.BaseRepositoryConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NuxeoRepositoryConnector extends BaseRepositoryConnector {

	// Tab properties
	private static final String NUXEO_SERVER_TAB_PROPERTY = "NuxeoRepositoryConnector.Server";

	// Prefix for nuxeo configuration and specification parameters
	private static final String PARAMETER_PREFIX = "nuxeo_";

	// Templates

	/**
	 * Javascript to check the configuration parameters
	 */
	private static final String EDIT_CONFIG_HEADER_FORWARD = "editConfiguration_conf.js";

	/**
	 * Server edit tab template
	 */
	private static final String EDIT_CONFIG_FORWARD_SERVER = "editConfiguration_conf_server.html";

	/**
	 * Server view tab template
	 */
	private static final String VIEW_CONFIG_FORWARD = "viewConfiguration_conf.html";

	private Logger logger = LoggerFactory.getLogger(NuxeoRepositoryConnector.class);

	/* Nuxeo instance parameters */
	protected String protocol = null;
	protected String host = null;
	protected String port = null;
	protected String path = null;
	protected String username = null;
	protected String password = null;

	public NuxeoRepositoryConnector() {
		super();
	}

	/** CONFIGURATION CONNECTOR **/
	@Override
	public void outputConfigurationHeader(IThreadContext threadContext, IHTTPOutput out, Locale locale,
			ConfigParams parameters, List<String> tabsArray) throws ManifoldCFException, IOException {

		// Server tab
		tabsArray.add(Messages.getString(locale, NUXEO_SERVER_TAB_PROPERTY));

		Map<String, String> paramMap = new HashMap<String, String>();

		// Fill in the parameters form each tab
		fillInServerConfigurationMap(paramMap, out, parameters);

		Messages.outputResourceWithVelocity(out, locale, EDIT_CONFIG_HEADER_FORWARD, paramMap, true);
	}

	@Override
	public void outputConfigurationBody(IThreadContext threadContext, IHTTPOutput out, Locale locale,
			ConfigParams parameters, String tabName) throws ManifoldCFException, IOException {

		// Call the Velocity tempaltes for each tab
		Map<String, String> paramMap = new HashMap<String, String>();

		// Set the tab name
		paramMap.put("TabName", tabName);

		// Fill in the parameters
		fillInServerConfigurationMap(paramMap, out, parameters);

		// Server tab
		Messages.outputResourceWithVelocity(out, locale, EDIT_CONFIG_FORWARD_SERVER, paramMap, true);

	}

	private static void fillInServerConfigurationMap(Map<String, String> serverMap, IPasswordMapperActivity mapper,
			ConfigParams parameters) {

		String nuxeoProtocol = parameters.getParameter(NuxeoConfiguration.Server.PROTOCOL);
		String nuxeoHost = parameters.getParameter(NuxeoConfiguration.Server.HOST);
		String nuxeoPort = parameters.getParameter(NuxeoConfiguration.Server.PORT);
		String nuxeoPath = parameters.getParameter(NuxeoConfiguration.Server.PATH);
		String nuxeoUsername = parameters.getParameter(NuxeoConfiguration.Server.USERNAME);
		String nuxeoPassword = parameters.getParameter(NuxeoConfiguration.Server.PASSWORD);

		if (nuxeoProtocol == null)
			nuxeoProtocol = NuxeoConfiguration.Server.PROTOCOL_DEFAULT_VALUE;
		if (nuxeoHost == null)
			nuxeoHost = NuxeoConfiguration.Server.HOST_DEFAULT_VALUE;
		if (nuxeoPort == null)
			nuxeoPort = NuxeoConfiguration.Server.PORT_DEFAULT_VALUE;
		if (nuxeoPath == null)
			nuxeoPath = NuxeoConfiguration.Server.PATH_DEFAULT_VALUE;
		if (nuxeoUsername == null)
			nuxeoUsername = NuxeoConfiguration.Server.USERNAME_DEFAULT_VALUE;
		if (nuxeoPassword == null)
			nuxeoPassword = NuxeoConfiguration.Server.PASSWORD_DEFAULT_VALUE;
		else
			nuxeoPassword = mapper.mapKeyToPassword(nuxeoPassword);

		serverMap.put(PARAMETER_PREFIX + NuxeoConfiguration.Server.PROTOCOL, nuxeoProtocol);
		serverMap.put(PARAMETER_PREFIX + NuxeoConfiguration.Server.HOST, nuxeoHost);
		serverMap.put(PARAMETER_PREFIX + NuxeoConfiguration.Server.PORT, nuxeoPort);
		serverMap.put(PARAMETER_PREFIX + NuxeoConfiguration.Server.PATH, nuxeoPath);
		serverMap.put(PARAMETER_PREFIX + NuxeoConfiguration.Server.USERNAME, nuxeoUsername);
		serverMap.put(PARAMETER_PREFIX + NuxeoConfiguration.Server.PASSWORD, nuxeoPassword);

	}

	@Override
	public String processConfigurationPost(IThreadContext thredContext, IPostParameters variableContext,
			ConfigParams parameters) {

		String nuxeoProtocol = variableContext.getParameter(PARAMETER_PREFIX + NuxeoConfiguration.Server.PROTOCOL);
		if (nuxeoProtocol != null)
			parameters.setParameter(NuxeoConfiguration.Server.PROTOCOL, nuxeoProtocol);

		String nuxeoHost = variableContext.getParameter(PARAMETER_PREFIX + NuxeoConfiguration.Server.HOST);
		if (nuxeoHost != null)
			parameters.setParameter(NuxeoConfiguration.Server.HOST, nuxeoHost);

		String nuxeoPort = variableContext.getParameter(PARAMETER_PREFIX + NuxeoConfiguration.Server.PORT);
		if (nuxeoPort != null)
			parameters.setParameter(NuxeoConfiguration.Server.PORT, nuxeoPort);

		String nuxeoPath = variableContext.getParameter(PARAMETER_PREFIX + NuxeoConfiguration.Server.PATH);
		if (nuxeoPath != null)
			parameters.setParameter(NuxeoConfiguration.Server.PATH, nuxeoPath);

		String nuxeoUsername = variableContext.getParameter(PARAMETER_PREFIX + NuxeoConfiguration.Server.USERNAME);
		if (nuxeoUsername != null)
			parameters.setParameter(NuxeoConfiguration.Server.USERNAME, nuxeoUsername);

		String nuxeoPassword = variableContext.getParameter(PARAMETER_PREFIX + NuxeoConfiguration.Server.PASSWORD);
		if (nuxeoPassword != null)
			parameters.setParameter(NuxeoConfiguration.Server.PASSWORD, nuxeoPassword);

		return null;
	}

	@Override
	public void viewConfiguration(IThreadContext threadContext, IHTTPOutput out, Locale locale, ConfigParams parameters)
			throws ManifoldCFException, IOException {

		Map<String, String> paramMap = new HashMap<String, String>();

		fillInServerConfigurationMap(paramMap, out, parameters);

		Messages.outputResourceWithVelocity(out, locale, VIEW_CONFIG_FORWARD, paramMap, true);
	}

	@Override
	public String check() throws ManifoldCFException {
		try {
			checkConnection();
			return super.check();
		} catch (ServiceInterruption serviceInterruption) {
			return "Connection temporarily failed:2" + serviceInterruption.getMessage();
		} catch (ManifoldCFException manifoldCFException) {
			return "Connection failed: " + manifoldCFException.getMessage();
		}
	}
	
	public void checkConnection() throws ManifoldCFException,ServiceInterruption{
		//TODO Check connection
	}
}
