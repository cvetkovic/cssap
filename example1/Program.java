package example1;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.spout.*;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.base.BaseWindowedBolt;

public class Program
{
	public static void main(String[] args)
	{
		TopologyBuilder builder = new TopologyBuilder();
		
		BaseRichSpout random = new RandomGeneratorSpout();
		BaseBasicBolt filter = new EvenNumberFilteringBolt();
		BaseBasicBolt summer = new RunningSumBolt();
		BaseBasicBolt bolt = new PrintingBolt();
		
		builder.setSpout("generator", random);
		builder.setBolt("filter", filter, 4).shuffleGrouping("generator");
		builder.setBolt("summer", summer).shuffleGrouping("filter");
		builder.setBolt("printer", bolt).shuffleGrouping("summer");
		
		Config config = new Config();
		config.setDebug(false);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Test", config, builder.createTopology());
	}
}