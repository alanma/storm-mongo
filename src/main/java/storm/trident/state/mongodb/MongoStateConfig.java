package storm.trident.state.mongodb;

import java.io.Serializable;

import storm.trident.state.StateType;

@SuppressWarnings("serial")
public class MongoStateConfig implements Serializable {

	private String url;
	private String db;
	private String collection;
	private StateType type = StateType.OPAQUE;
	private String[] keyFields;
	private boolean bulkGets;
	private int cacheSize = DEFAULT;

	private static final int DEFAULT = 5000;

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getDb() {
		return db;
	}

	public void setDb(String db) {
		this.db = db;
	}

	public String getCollection() {
		return collection;
	}

	public void setCollection(String collection) {
		this.collection = collection;
	}

	public StateType getType() {
		return type;
	}

	public void setType(StateType type) {
		this.type = type;
	}

	public String[] getKeyFields() {
		return keyFields;
	}

	public void setKeyFields(String[] keyFields) {
		this.keyFields = keyFields;
	}

	public boolean isBulkGets() {
		return bulkGets;
	}

	public void setBulkGets(boolean bulkGets) {
		this.bulkGets = bulkGets;
	}

	public int getCacheSize() {
		return cacheSize;
	}

	public void setCacheSize(int cacheSize) {
		this.cacheSize = cacheSize;
	}

}
