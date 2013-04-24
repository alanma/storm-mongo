package storm.trident.state.mongodb;

import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import storm.trident.state.OpaqueValue;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.TransactionalValue;
import storm.trident.state.map.CachedMap;
import storm.trident.state.map.IBackingMap;
import storm.trident.state.map.NonTransactionalMap;
import storm.trident.state.map.OpaqueMap;
import storm.trident.state.map.TransactionalMap;
import backtype.storm.task.IMetricsContext;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

public class MongoState<T> implements IBackingMap<T> {

	private DBCollection collection;
	private MongoStateConfig config;
	private static final Logger logger = Logger.getLogger(MongoState.class);

	MongoState(final MongoStateConfig config) {
		this.config = config;
		// initialize the mongo client
		try {
			collection = new MongoClient(new MongoClientURI(config.getUrl())).getDB(config.getDb()).getCollection(config.getCollection());
		} catch (final UnknownHostException ex) {
			logger.error("DB connection initialization failed.", ex);
		}
	}

	/**
	 * factory method for the factory
	 * 
	 * @param config
	 * @return
	 */
	public static Factory newFactory(final MongoStateConfig config) {
		return new Factory(config);
	}

	/**
	 * multiget implementation for mongodb
	 * 
	 */
	@Override
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public List<T> multiGet(final List<List<Object>> keys) {
		// convert all keys into mongodb id docs
		final List<DBObject> ids = keysToIds(keys);
		List<DBObject> docs = null;
		if (config.isBulkGets()) {
			docs = batchGet(ids);
		} else {
			docs = singleGet(ids);
		}
		// convert the docs into a map so we can figure out which keys are
		// present
		final Map<DBObject, DBObject> docMap = Maps.uniqueIndex(docs, new Function<DBObject, DBObject>() {
			@Override
			public DBObject apply(final DBObject doc) {
				return (DBObject) doc.get("_id");
			}
		});
		return Lists.transform(ids, new Function<DBObject, T>() {
			@Override
			public T apply(final DBObject id) {
				final DBObject doc = docMap.get(id);
				if (doc == null) {
					return null;
				} else {
					switch (config.getType()) {
					case OPAQUE:
						return (T) new OpaqueValue((Long) doc.get("txid"), doc.get("val"), doc.get("prev"));
					case TRANSACTIONAL:
						return (T) new TransactionalValue((Long) doc.get("txid"), doc.get("val"));
					default:
						return (T) doc.get("val");
					}
				}
			}
		});
	}

	/**
	 * multiput implementation for mongodb
	 * 
	 */
	@Override
	public void multiPut(final List<List<Object>> keys, final List<T> values) {
		// convert all keys into mongodb id docs
		final List<DBObject> ids = keysToIds(keys);
		for (int i = 0; i < ids.size(); i++) {
			// perform an upsert
			collection.update(new BasicDBObject("_id", ids.get(i)), tuplesToDocument(ids.get(i), values.get(i)), true, false);
		}
	}

	/**
	 * retrieve all keys in one request
	 * 
	 * @param ids
	 * @return
	 */
	private List<DBObject> batchGet(final List<DBObject> ids) {
		final DBCursor cursor = collection.find(new BasicDBObject("_id", new BasicDBObject("$in", ids)));
		try {
			return cursor.toArray();
		} finally {
			if (cursor != null) {
				cursor.close();
			}
		}
	}

	/**
	 * retrieve all keys by issuing single requests iteratively
	 * 
	 * @param ids
	 * @return
	 */
	private List<DBObject> singleGet(final List<DBObject> ids) {
		return Lists.transform(ids, new Function<DBObject, DBObject>() {
			@Override
			public DBObject apply(final DBObject id) {
				return collection.findOne(new BasicDBObject("id", id));
			}
		});
	}

	/**
	 * convert trident keys to mongodb docs repesenting the document id
	 * 
	 * @param keys
	 * @return
	 */
	private List<DBObject> keysToIds(final List<List<Object>> keys) {
		return Lists.transform(keys, new Function<List<Object>, DBObject>() {
			@Override
			public DBObject apply(final List<Object> key) {
				final BasicDBObject id = new BasicDBObject(key.size());
				for (int i = 0; i < config.getKeyFields().length; i++) {
					id.put(config.getKeyFields()[i], key.get(i));
				}
				return id;
			}
		});
	}

	/**
	 * convert an id, value pair to a mongo doc
	 * 
	 * @param id
	 * @param value
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	private DBObject tuplesToDocument(final DBObject id, final T value) {
		final BasicDBObject doc = new BasicDBObject("_id", id);
		switch (config.getType()) {
		case OPAQUE:
			doc.append("val", ((OpaqueValue) value).getCurr()).append("txid", ((OpaqueValue) value).getCurrTxid())
					.append("prev", ((OpaqueValue) value).getPrev());
			break;
		case TRANSACTIONAL:
			doc.append("val", ((TransactionalValue) value).getVal()).append("txid", ((TransactionalValue) value).getTxid());
			break;
		default:
			doc.append("val", value);
		}
		return doc;
	}

	@SuppressWarnings("serial")
	static class Factory implements StateFactory {
		private MongoStateConfig config;

		Factory(final MongoStateConfig config) {
			this.config = config;
		}

		@Override
		@SuppressWarnings({ "rawtypes", "unchecked" })
		public State makeState(final Map conf, final IMetricsContext context, final int partitionIndex, final int numPartitions) {
			final CachedMap map = new CachedMap(new MongoState(config), config.getCacheSize());
			switch (config.getType()) {
			case OPAQUE:
				return OpaqueMap.build(map);
			case TRANSACTIONAL:
				return TransactionalMap.build(map);
			default:
				return NonTransactionalMap.build(map);
			}
		}
	}
}
