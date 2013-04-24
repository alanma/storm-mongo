package storm.trident.mongodb;

import java.util.Map;
import java.util.Random;

import storm.trident.TridentTopology;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Sum;
import storm.trident.spout.IBatchSpout;
import storm.trident.state.StateType;
import storm.trident.state.mongodb.MongoState;
import storm.trident.state.mongodb.MongoStateConfig;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class MongoStateTopology {

	@SuppressWarnings("serial")
	static class RandomTupleSpout implements IBatchSpout {
		private transient Random random;
		private static final int BATCH = 5000;

		@Override
		@SuppressWarnings("rawtypes")
		public void open(final Map conf, final TopologyContext context) {
			random = new Random();
		}

		@Override
		public void emitBatch(final long batchId, final TridentCollector collector) {
			// emit a 3 number tuple (a,b,c)
			for (int i = 0; i < BATCH; i++) {
				collector.emit(new Values(random.nextInt(1000) + 1, random.nextInt(100) + 1, random.nextInt(100) + 1));
			}
		}

		@Override
		public void ack(final long batchId) {
		}

		@Override
		public void close() {
		}

		@Override
		@SuppressWarnings("rawtypes")
		public Map getComponentConfiguration() {
			return null;
		}

		@Override
		public Fields getOutputFields() {
			return new Fields("a", "b", "c");
		}
	}

	/**
	 * storm local cluster executable
	 * 
	 * @param args
	 */
	public static void main(final String[] args) {
		final MongoStateConfig config = new MongoStateConfig();
		{
			config.setUrl("mongodb://localhost");
			config.setDb("test");
			config.setCollection("state");
			config.setBulkGets(true);
			config.setKeyFields(new String[] { "a" });
			config.setType(StateType.NON_TRANSACTIONAL);
			config.setCacheSize(1000);
		}
		final TridentTopology topology = new TridentTopology();
		topology.newStream("test", new RandomTupleSpout()).groupBy(new Fields("a"))
				.persistentAggregate(MongoState.newFactory(config), new Fields("b"), new Sum(), new Fields("bsum"));
		final LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", new Config(), topology.build());
		while (true) {
			
		}
	}
}
