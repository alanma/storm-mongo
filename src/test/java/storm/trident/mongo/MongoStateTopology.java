package storm.trident.mongo;

import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;

import storm.trident.TridentTopology;
import storm.trident.fluent.GroupedStream;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Sum;
import storm.trident.spout.IBatchSpout;
import storm.trident.state.StateType;
import storm.trident.state.mongo.MongoState;
import storm.trident.state.mongo.MongoStateConfig;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import clojure.lang.Numbers;

import com.google.common.collect.Lists;

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
		public void ack(final long batchId) {}

		@Override
		public void close() {}

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

	@SuppressWarnings("serial")
	static class LoggingFilter extends BaseFilter {

		private static final Logger logger = Logger.getLogger(LoggingFilter.class);

		public boolean isKeep(final TridentTuple tuple) {
			logger.info(tuple);
			return true;
		}
	}

	@SuppressWarnings("serial")
	static class CountSumSum implements CombinerAggregator<List<Number>> {

		@Override
		public List<Number> init(TridentTuple tuple) {
			return Lists.newArrayList(1L, (Number) tuple.getValue(0), (Number) tuple.getValue(1));
		}

		@Override
		public List<Number> combine(List<Number> val1, List<Number> val2) {
			return Lists.newArrayList(Numbers.add(val1.get(0), val2.get(0)), Numbers.add(val1.get(1), val2.get(1)), Numbers.add(val1.get(2), val2.get(2)));
		}

		@Override
		public List<Number> zero() {
			return Lists.newArrayList((Number) 0, (Number) 0, (Number) 0);
		}

	}

	/**
	 * storm local cluster executable
	 * 
	 * @param args
	 */
	public static void main(final String[] args) {
		final TridentTopology topology = new TridentTopology();
		final GroupedStream stream = topology.newStream("test", new RandomTupleSpout()).groupBy(new Fields("a"));
			// .aggregate(new Fields("b", "c"), new CountSumSum(), new Fields("sum"))
			// .each(new Fields("a", "sum"), new LoggingFilter());
		final MongoStateConfig config = new MongoStateConfig();
		{
			config.setUrl("mongodb://localhost");
			config.setDb("test");
			config.setCollection("state");
			config.setBulkGets(true);
			config.setKeyFields(new String[]{"a"});
			config.setValueFields(new String[]{"count","sumb","sumc"});
			config.setType(StateType.NON_TRANSACTIONAL);
			config.setCacheSize(1000);
		}
		stream.persistentAggregate(MongoState.newFactory(config), new Fields("b", "c"), new CountSumSum(), new Fields("sum"));
		
		final MongoStateConfig config1 = new MongoStateConfig();
		{
			config1.setUrl("mongodb://localhost");
			config1.setDb("test");
			config1.setCollection("state1");
			config1.setBulkGets(true);
			config1.setKeyFields(new String[]{"a"});
			config1.setValueFields(new String[]{"csum"});
			config1.setType(StateType.NON_TRANSACTIONAL);
			config1.setCacheSize(1000);
		}
		stream.persistentAggregate(MongoState.newFactory(config1), new Fields("c"), new Sum(), new Fields("sum"));
		final LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", new Config(), topology.build());
		while (true) {

		}
	}
}
