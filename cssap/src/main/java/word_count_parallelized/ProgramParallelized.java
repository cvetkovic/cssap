package word_count_parallelized;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;

public class ProgramParallelized
{
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();

		BaseRichSpout generator = new WordGenerator();
		BaseRichBolt splitter = new Splitter();
		BaseBasicBolt counter = new Counter();
		BaseBasicBolt printer = new Printer();

		builder.setSpout("generator", generator);
		builder.setBolt("splitter", splitter, 10).shuffleGrouping("generator");
		builder.setBolt("counter", counter, 26).directGrouping("splitter");
		builder.setBolt("printer", printer).globalGrouping("counter");

		Config config = new Config();
		config.setDebug(false);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Test", config, builder.createTopology());
	}
}