package org.apache.manifoldcf.crawler.connectors.nuxeo;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;

import org.apache.commons.lang.StringUtils;
import org.apache.manifoldcf.agents.interfaces.RepositoryDocument;
import org.apache.manifoldcf.agents.interfaces.ServiceInterruption;
import org.apache.manifoldcf.core.interfaces.ConfigParams;
import org.apache.manifoldcf.core.interfaces.IHTTPOutput;
import org.apache.manifoldcf.core.interfaces.IPasswordMapperActivity;
import org.apache.manifoldcf.core.interfaces.IPostParameters;
import org.apache.manifoldcf.core.interfaces.IThreadContext;
import org.apache.manifoldcf.core.interfaces.ManifoldCFException;
import org.apache.manifoldcf.core.interfaces.Specification;
import org.apache.manifoldcf.crawler.connectors.BaseRepositoryConnector;
import org.apache.manifoldcf.crawler.connectors.nuxeo.client.NuxeoClient;
import org.apache.manifoldcf.crawler.connectors.nuxeo.model.Document;
import org.apache.manifoldcf.crawler.connectors.nuxeo.model.NuxeoResponse;
import org.apache.manifoldcf.crawler.interfaces.IExistingVersions;
import org.apache.manifoldcf.crawler.interfaces.IProcessActivity;
import org.apache.manifoldcf.crawler.interfaces.IRepositoryConnector;
import org.apache.manifoldcf.crawler.interfaces.ISeedingActivity;

import com.google.common.collect.Maps;

public class NuxeoRepositoryConnector extends BaseRepositoryConnector {

	protected final static String ACTIVITY_READ = "read document";

	/** Deny access token for default authority */
	// private final static String defaultAuthorityDenyToken =
	// GLOBAL_DENY_TOKEN;

	// Configuration tabs
	private static final String NUXEO_SERVER_TAB_PROPERTY = "NuxeoRepositoryConnector.Server";

	// Configurationt tabs

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

	protected long lastSessionFetch = -1L;
	protected static final long timeToRelease = 300000L;

	// private Logger logger =
	// LoggerFactory.getLogger(NuxeoRepositoryConnector.class);

	/* Nuxeo instance parameters */
	protected String protocol = null;
	protected String host = null;
	protected String port = null;
	protected String path = null;
	protected String username = null;
	protected String password = null;

	protected NuxeoClient nuxeoClient = null;

	public NuxeoRepositoryConnector() {
		super();
	}

	public void setNuxeoClient(NuxeoClient nuxeoClient) {
		this.nuxeoClient = nuxeoClient;
	}

	@Override
	public String[] getActivitiesList() {
		return new String[] { ACTIVITY_READ };
	}

	@Override
	public String[] getBinNames(String documentIdenfitier) {
		return new String[] { host };
	}

	public void disconenct() throws ManifoldCFException {
		if (nuxeoClient != null)
			nuxeoClient = null;

		protocol = null;
		host = null;
		port = null;
		path = null;
		username = null;
		password = null;
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
			parameters.setObfuscatedParameter(NuxeoConfiguration.Server.PASSWORD,
					variableContext.mapKeyToPassword(nuxeoPassword));

		return null;
	}

	@Override
	public void viewConfiguration(IThreadContext threadContext, IHTTPOutput out, Locale locale, ConfigParams parameters)
			throws ManifoldCFException, IOException {

		Map<String, String> paramMap = new HashMap<String, String>();

		fillInServerConfigurationMap(paramMap, out, parameters);

		Messages.outputResourceWithVelocity(out, locale, VIEW_CONFIG_FORWARD, paramMap, true);
	}

	/** CONNECTION **/
	@Override
	public void connect(ConfigParams configParams) {
		super.connect(configParams);

		protocol = params.getParameter(NuxeoConfiguration.Server.PROTOCOL);
		host = params.getParameter(NuxeoConfiguration.Server.HOST);
		port = params.getParameter(NuxeoConfiguration.Server.PORT);
		path = params.getParameter(NuxeoConfiguration.Server.PATH);
		username = params.getParameter(NuxeoConfiguration.Server.USERNAME);
		password = params.getObfuscatedParameter(NuxeoConfiguration.Server.PASSWORD);

		try {
			initNuxeoClient();
		} catch (ManifoldCFException manifoldCFException) {
			manifoldCFException.printStackTrace();
		}
	}

	@Override
	public String check() throws ManifoldCFException {
		try {
			if (!isConnected()) {
				initNuxeoClient();
			}

			Boolean result = nuxeoClient.check();

			if (result)
				return super.check();
			else
				throw new ManifoldCFException("Nuxeo instance could not be reached");

		} catch (ServiceInterruption serviceInterruption) {
			return "Connection temporarily failed: " + serviceInterruption.getMessage();
		} catch (ManifoldCFException manifoldCFException) {
			return "Connection failed: " + manifoldCFException.getMessage();
		} catch (Exception e) {
			return "Connection failed: " + e.getMessage();
		}
	}

	private void initNuxeoClient() throws ManifoldCFException {
		int portInt;

		if (nuxeoClient == null) {

			if (StringUtils.isEmpty(protocol)) {
				throw new ManifoldCFException(
						"Parameter " + NuxeoConfiguration.Server.PROTOCOL + " required but not set");
			}

			if (StringUtils.isEmpty(host)) {
				throw new ManifoldCFException("Parameter " + NuxeoConfiguration.Server.HOST + " required but not set");
			}

			if (port != null && port.length() > 0) {
				try {
					portInt = Integer.parseInt(port);
				} catch (NumberFormatException formatException) {
					throw new ManifoldCFException("Bad number: " + formatException.getMessage(), formatException);
				}
			} else {
				if (protocol.toLowerCase(Locale.ROOT).equals("http")) {
					portInt = 80;
				} else {
					portInt = 443;
				}
			}

			nuxeoClient = new NuxeoClient(protocol, host, portInt, path, username, password);

			lastSessionFetch = System.currentTimeMillis();

		}

	}

	@Override
	public boolean isConnected() {
		return nuxeoClient != null;
	}

	@Override
	public void poll() throws ManifoldCFException {
		if (lastSessionFetch == -1L) {
			return;
		}

		long currentTime = System.currentTimeMillis();

		if (currentTime > lastSessionFetch + timeToRelease) {
			nuxeoClient.close();
			nuxeoClient = null;
			lastSessionFetch = -1;
		}
	}

	/** SEEDING **/

	@Override
	public String addSeedDocuments(ISeedingActivity activities, Specification spec, String lastSeedVersion,
			long seedTime, int jobMode) throws ManifoldCFException, ServiceInterruption {

		if (!isConnected())
			initNuxeoClient();

		try {

			int lastStart = 0;
			int defaultSize = 50;
			Boolean isLast = true;

			do {
				final NuxeoResponse<Document> response = nuxeoClient.getDocuments(lastSeedVersion, lastStart,
						defaultSize, isLast);

				for (Document doc : response.getResults()) {
					activities.addSeedDocument(doc.getUid());
				}

				lastStart++;
				isLast = response.isLast();

			} while (!isLast);

			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
			sdf.setTimeZone(TimeZone.getTimeZone("GMT"));

			lastSeedVersion = sdf.format(new Date());

			return lastSeedVersion;
		} catch (Exception exception) {
			long interruptionRetryTime = 5L * 60L * 1000L;
			String message = "Server appears down during seeding: " + exception.getMessage();
			throw new ServiceInterruption(message, exception, System.currentTimeMillis() + interruptionRetryTime, -1L,
					3, true);
		}
	}

	// TDO Specification
	// public static class NuxeoSpecification {
	//
	// private List<String> directories;
	//
	// /**
	// * @param spec
	// * @return
	// */
	// public static NuxeoSpecification from(Specification spec) {
	// NuxeoSpecification ns = new NuxeoSpecification();
	//
	// ns.directories = Lists.newArrayList();
	//
	// for (int i = 0, len = spec.getChildCount(); i < len; i++) {
	// SpecificationNode specificationNode = spec.getChild(i);
	//
	// }
	//
	// return null;
	// }
	//
	// }

	/** PROCESS DOCUMENTS **/
	@Override
	public void processDocuments(String[] documentsIdentifieres, IExistingVersions statuses, Specification spec,
			IProcessActivity activities, int jobMode, boolean usesDefaultAuthority)
			throws ManifoldCFException, ServiceInterruption {

		for (int i = 0; i < documentsIdentifieres.length; i++) {

			String documentId = documentsIdentifieres[i];
			String version = statuses.getIndexedVersionString(documentId);

			long startTime = System.currentTimeMillis();
			String errorCode = "OK";
			String errorDesc = StringUtils.EMPTY;
			ProcessResult pResult = null;
			boolean doLog = true;

			try {

				if (!isConnected()) {
					initNuxeoClient();
				}

				pResult = processDocument(documentId, version, activities, doLog, Maps.<String, String> newHashMap());
			} catch (Exception exception) {
				long interruptionRetryTime = 5L * 60L * 1000L;
				String message = "Server appears down during seeding: " + exception.getMessage();
				throw new ServiceInterruption(message, exception, System.currentTimeMillis() + interruptionRetryTime,
						-1L, 3, true);
			} finally {
				if (doLog)
					if (pResult.errorCode != null && !pResult.errorCode.isEmpty()) {
						activities.recordActivity(new Long(startTime), ACTIVITY_READ, pResult.fileSize, documentId,
								pResult.errorCode, pResult.errorDecription, null);
					} else {
						activities.recordActivity(new Long(startTime), ACTIVITY_READ, pResult.fileSize, documentId,
								errorCode, errorDesc, null);
					}
			}

		}
	}

	/**
	 * @param documentId
	 * @param version
	 * @param activities
	 * @param doLog
	 * @param newHashMap
	 * @return
	 */
	private ProcessResult processDocument(String documentId, String version, IProcessActivity activities, boolean doLog,
			HashMap<String, String> extraProperties) throws ManifoldCFException, ServiceInterruption, IOException {

		Document doc = nuxeoClient.getDocument(documentId);

		return processDocumentInternal(doc, documentId, version, activities, doLog, extraProperties);
	}

	/**
	 * @param doc
	 * @param documentId
	 * @param version
	 * @param activities
	 * @param doLog
	 * @param extraProperties
	 * @return
	 */
	private ProcessResult processDocumentInternal(Document doc, String manifoldDocumentIdentifier, String version,
			IProcessActivity activities, boolean doLog, HashMap<String, String> extraProperties)
			throws ManifoldCFException, ServiceInterruption, IOException {

		RepositoryDocument rd = new RepositoryDocument();

		Date lastModified = doc.getLastModified();

		DateFormat df = DateFormat.getDateTimeInstance();

		String lastVersion = null;

		if (lastModified != null)
			lastVersion = df.format(lastModified);

		if (doc.getState() != null && doc.getState().equalsIgnoreCase(Document.DELETED)) {
			activities.deleteDocument(manifoldDocumentIdentifier);
			return new ProcessResult(doc.getLenght(), "DELETED", "");
		}

		if (!activities.checkDocumentNeedsReindexing(manifoldDocumentIdentifier, lastVersion)) {
			return new ProcessResult(doc.getLenght(), "RETAINED", "");
		}

		// Add respository document information
		rd.setMimeType("text/html; charset=utf-8");
		if (lastModified != null)
			rd.setModifiedDate(lastModified);
		rd.setIndexingDate(new Date());

		// Adding Document Metadata
		Map<String, Object> docMetadata = doc.getMetadataAsMap();

		for (Entry<String, Object> entry : docMetadata.entrySet()) {
			if (entry.getValue() instanceof List) {
				List<?> list = (List<?>) entry.getValue();
				rd.addField(entry.getKey(), list.toArray(new String[list.size()]));
			} else {
				rd.addField(entry.getKey(), entry.getValue().toString());
			}

		}

		String documentUri = nuxeoClient.getPathDocument(doc.getUid());

		// Set repository ACLs
		// TODO ACLs

		rd.setBinary(doc.getContentStream(), doc.getLenght());

		// Action
		activities.ingestDocumentWithException(manifoldDocumentIdentifier, lastVersion, documentUri, rd);

		return new ProcessResult(doc.getLenght(), null, null);
	}

	public String processSpecificationPost(IPostParameters variableContext, Locale locale, Specification ds,
			int connectionSequenceNumber) throws ManifoldCFException {

		// TODO Specifications
		return super.processSpecificationPost(variableContext, locale, ds, connectionSequenceNumber);
	}

	private class ProcessResult {
		private long fileSize;
		private String errorCode;
		private String errorDecription;

		private ProcessResult(long fileSize, String errorCode, String errorDescription) {
			this.fileSize = fileSize;
			this.errorCode = errorCode;
			this.errorDecription = errorDescription;
		}

	}

	@Override
	public int getConnectorModel() {
		return IRepositoryConnector.MODEL_ADD_CHANGE_DELETE;
	}
}
