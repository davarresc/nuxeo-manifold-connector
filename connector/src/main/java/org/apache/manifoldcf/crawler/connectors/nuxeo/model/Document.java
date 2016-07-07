package org.apache.manifoldcf.crawler.connectors.nuxeo.model;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Map;

import org.apache.manifoldcf.core.common.DateParser;
import org.apache.manifoldcf.crawler.connectors.nuxeo.model.builder.NuxeoResourceBuilder;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.common.collect.Maps;

public class Document extends NuxeoResource {

	public static final String DELETED = "deleted";
	// KEYS
	protected static final String KEY_UID = "uid";
	protected static final String KEY_TITLE = "title";
	protected static final String KEY_LAST_MODIFIED = "lastModified";
	protected static final String KEY_STATE = "state";
	protected static final String KEY_TYPE = "type";
	protected static final String KEY_PATH = "path";
	protected static final String KEY_MEDIATYPE = "mediaType";
	protected static final String KEY_IS_CHECKED_OUT = "isCheckedOut";
	protected static final String KEY_PARENT_REF = "parentRef";
	protected static final String KEY_REPOSITORY = "repository";

	protected static final String KEY_PROPERTIES = "properties";

	protected static final String DOCUMENT_SIZE = "size";

	// Attributes
	protected String uid;
	protected String title;
	protected Date lastModified;
	protected String state;
	protected String mediatype = "text/html; charset=utf-8";
	protected long length;
	protected String content;
	protected String path;
	protected String type;
	protected Boolean isCheckedOut;
	protected String parentRef;
	protected String repository;

	@SuppressWarnings("unused")
	private JSONObject delegated;

	public Document() {

	}

	// Getters
	public String getUid() {
		return uid;
	}

	public String getTitle() {
		return title;
	}

	public Date getLastModified() {
		return lastModified;
	}

	public String getState() {
		return state;
	}

	public String getPath() {
		return path;
	}

	public String getType() {
		return type;
	}

	public String getMediatype() {
		return this.mediatype;
	}

	public Boolean getIsCheckedOut() {
		return isCheckedOut;
	}

	public String getParentRef() {
		return parentRef;
	}

	public String getRepository() {
		return repository;
	}

	public long getLenght() {
		return this.length;
	}

	public boolean hasContent() {
		return this.length > 0 && this.content != null;
	}

	public InputStream getContentStream() {
		String contentStream = content != null ? content : "";
		return new ByteArrayInputStream(contentStream.getBytes(StandardCharsets.UTF_8));
	}

	public Map<String, Object> getMetadataAsMap() {
		Map<String, Object> docMetadata = Maps.newHashMap();

		docMetadata.put(KEY_UID, this.uid);
		docMetadata.put(KEY_TITLE, this.title);
		if (this.lastModified != null)
			docMetadata.put(KEY_LAST_MODIFIED, DateParser.formatISO8601Date(this.lastModified));
		docMetadata.put(KEY_STATE, this.state);
		docMetadata.put(KEY_MEDIATYPE, this.mediatype);
		docMetadata.put(KEY_TYPE, this.type);
		docMetadata.put(KEY_PATH, this.path);
		if (this.isCheckedOut != null)
			docMetadata.put(KEY_IS_CHECKED_OUT, this.isCheckedOut.toString());
		docMetadata.put(KEY_REPOSITORY, this.repository);
		docMetadata.put(KEY_PARENT_REF, this.parentRef);

		return docMetadata;
	}

	public static NuxeoResourceBuilder<? extends Document> builder() {
		return new PageBuilder();
	}

	public static class PageBuilder implements NuxeoResourceBuilder<Document> {

		public Document fromJson(JSONObject jsonDocument) {
			return fromJson(jsonDocument, new Document());
		}

		public Document fromJson(JSONObject jsonDocument, Document document) {

			try {
				String uid = jsonDocument.getString(KEY_UID);
				String tilte = jsonDocument.getString(KEY_TITLE);
				Date lastModified = DateParser.parseISO8601Date(jsonDocument.optString(KEY_LAST_MODIFIED, ""));
				String state = jsonDocument.optString(KEY_STATE, "");
				String path = jsonDocument.optString(KEY_PATH, "");
				String type = jsonDocument.optString(KEY_TYPE, "");
				Boolean isCheckedOut = jsonDocument.optBoolean(KEY_IS_CHECKED_OUT);
				String repository = jsonDocument.optString(KEY_REPOSITORY, "");
				String parentRef = jsonDocument.optString(KEY_PARENT_REF, "");

				document.uid = uid;
				document.title = tilte;
				document.lastModified = lastModified;
				document.state = state;
				document.path = path;
				document.type = type;
				document.isCheckedOut = isCheckedOut;
				document.repository = repository;
				document.parentRef = parentRef;

				if (document.content != null)
					document.length = document.content.getBytes().length;

				// JSONObject properties = (JSONObject)
				// jsonDocument.opt(KEY_PROPERTIES);

				document.delegated = jsonDocument;

				return document;

			} catch (JSONException e) {
				e.printStackTrace();
			}

			return new Document();

		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.manifoldcf.crawler.connectors.nuxeo.model.builder.
		 * NuxeoResourceBuilder#getType()
		 */
		@Override
		public Class<Document> getType() {
			return Document.class;
		}

	}
}
