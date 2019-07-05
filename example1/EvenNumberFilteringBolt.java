package example1;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class EvenNumberFilteringBolt extends BaseBasicBolt
{

	public void execute(Tuple input, BasicOutputCollector collector)
	{
		// TODO Auto-generated method stub
		int number = input.getIntegerByField("id1");
		if (number % 2 == 0)
		{
			collector.emit(new Values(input.getInteger(0), Math.abs(input.getDouble(1))));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		// TODO Auto-generated method stub
		declarer.declare(new Fields("id2", "value2"));
	}

}