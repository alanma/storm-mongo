package storm.trident.state.mongo;

import java.io.Serializable;

import storm.trident.state.StateType;

@SuppressWarnings("serial")
public class MongoStateConfig implements Serializable {

	private String url;
	private String db;
	private String collection;
	private StateType type = StateType.OPAQUE;
	private String[] keyFields;
	private String[] valueFields;
	private boolean bulkGets = true;
	private int cacheSize = DEFAULT;

	private static final int DEFAULT = 5000;
	
	public MongoStateConfig() {}
	
	public MongoStateConfig(final String url, final String db, final String collection, final StateType type, final String[] keyFields, final String[] valueFields) {
		this.url = url;
		this.db = db;
		this.collection = collection;
		this.type = type;
		this.keyFields = keyFields;
		this.valueFields = valueFields;
	}

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

	public String[] getValueFields() {
		return valueFields;
	}

	public void setValueFields(String[] valueFields) {
		this.valueFields = valueFields;
	}

}
