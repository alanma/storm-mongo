package storm.trident.mongo

import scala.collection.JavaConversions.seqAsJavaList
import scala.collection.immutable.List
import scala.util.Random

import org.apache.log4j.Logger

import backtype.storm.Config
import backtype.storm.LocalCluster
import backtype.storm.task.TopologyContext
import backtype.storm.tuple.Fields
import clojure.lang.Numbers
import storm.trident.TridentTopology
import storm.trident.fluent.GroupedStream
import storm.trident.operation.BaseFilter
import storm.trident.operation.CombinerAggregator
import storm.trident.operation.TridentCollector
import storm.trident.spout.IBatchSpout
import storm.trident.state.StateType
import storm.trident.state.mongo.MongoState
import storm.trident.state.mongo.MongoStateConfig
import storm.trident.tuple.TridentTuple

object TestTopology {

	class RandomTupleSpout extends IBatchSpout {
		@transient var random: Random = null
		val BATCH = 5000

		override def open(conf: java.util.Map[_, _], context: TopologyContext): Unit = {
			random = new Random
		}

		override def emitBatch(batchId: Long, collector: TridentCollector) = {
			(1 to BATCH).foreach(x => {
				val values = toJavaList[Int, Object](List(random.nextInt(1000) + 1, random.nextInt(100) + 1, random.nextInt(100) + 1))
				collector.emit(values)
			})
		}

		override def ack(batchId: Long) = ()

		override def close() = ()

		override def getComponentConfiguration() = null

		override def getOutputFields() = new Fields("a", "b", "c")
	}

	class ThroughputLoggingFilter extends BaseFilter {

		var count: Long = 0
		var start: Long = System.nanoTime
		var last: Long = System.nanoTime

		override def isKeep(tuple: TridentTuple) = {
			count += 1
			val now = System.nanoTime
			if (now - last > 5000000000L) { // emit every 5 seconds
				Logger.getLogger(this.getClass).info("tuples per second = " + (count * 1000000000L) / (now - start))
				last = now
			}
			true
		}
	}

	object CountSumSum extends CombinerAggregator[java.util.List[Number]] {
		override def init(tuple: TridentTuple) = toJavaList[Number, Number](List(1.asInstanceOf[Number], tuple.get(0).asInstanceOf[Number], tuple.get(1).asInstanceOf[Number]))

		override def combine(v1: java.util.List[Number], v2: java.util.List[Number]): java.util.List[Number] = toJavaList[Number, Number](List(
			Numbers.add(v1.get(0), v2.get(0)), Numbers.add(v1.get(1), v2.get(1)), Numbers.add(v1.get(2), v2.get(2))))

		override def zero(): java.util.List[Number] = toJavaList[Int, Number](List(0, 0, 0))
	}

	def toJavaList[U, T](x: List[U]): java.util.List[T] = seqAsJavaList(x).asInstanceOf[java.util.List[T]]

	def main(args: Array[String]): Unit = {
		val topology: TridentTopology = new TridentTopology
		val stream: GroupedStream = topology.newStream("test", new RandomTupleSpout).each(new Fields("a", "b", "c"), new ThroughputLoggingFilter).groupBy(new Fields("a"))
		val config: MongoStateConfig =
			new MongoStateConfig("mongodb://localhost", "test", "state", StateType.NON_TRANSACTIONAL, Array[String]("a"), Array[String]("count", "sumb", "sumc"));
		stream.persistentAggregate(MongoState.newFactory(config), new Fields("b", "c"), CountSumSum, new Fields("summary"));

		val configTransactional: MongoStateConfig =
			new MongoStateConfig("mongodb://localhost", "test", "state_transactional", StateType.TRANSACTIONAL, Array[String]("a"), Array[String]("count", "sumb", "sumc"));
		stream.persistentAggregate(MongoState.newFactory(configTransactional), new Fields("b", "c"), CountSumSum, new Fields("summary"));

		val configOpaque: MongoStateConfig =
			new MongoStateConfig("mongodb://localhost", "test", "state_opaque", StateType.OPAQUE, Array[String]("a"), Array[String]("count", "sumb", "sumc"));
		stream.persistentAggregate(MongoState.newFactory(configOpaque), new Fields("b", "c"), CountSumSum, new Fields("summary"));

		val conf = new Config
		conf.setMaxSpoutPending(1)
		new LocalCluster().submitTopology("test", conf, topology.build)
		while (true) {
		}
	}
}

