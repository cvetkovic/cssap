package example1;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class PrintingBolt extends BaseBasicBolt
{

	public void execute(Tuple input, BasicOutputCollector collector)
	{
		// TODO Auto-generated method stub
		System.out.println(input);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		// TODO Auto-generated method stub
		
	}

}