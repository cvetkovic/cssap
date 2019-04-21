package amplitude_modulation;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;

import example1.EvenNumberFilteringBolt;
import example1.PrintingBolt;
import example1.RandomGeneratorSpout;
import example1.RunningSumBolt;

public class Program
{

	public static void main(String[] args)
	{
		TopologyBuilder builder = new TopologyBuilder();
	
		double f=1000;
		
		BaseRichSpout signal = new ModulatingSignalGenerator(f, -1, 1);
		BaseRichSpout carrier = new CarrierGenerator(f, 1, 167, 0);
		
		BaseBasicBolt multiplier = new Multiplier();
		BaseBasicBolt output = new OutputSignalWriter();
		
		builder.setSpout("signal", signal);
		builder.setSpout("carrier", carrier);
		builder.setBolt("multiplier", multiplier, 4)
			.fieldsGrouping("signal", new Fields("order_number"))
			.fieldsGrouping("carrier", new Fields("order_number"));
		builder.setBolt("output", output).shuffleGrouping("multiplier");
		
		Config config = new Config();
		config.setDebug(false);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Test", config, builder.createTopology());
	}

}
