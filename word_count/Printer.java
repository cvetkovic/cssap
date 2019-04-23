package word_count;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

public class Printer extends BaseBasicBolt
{
	@Override
	public void execute(Tuple input, BasicOutputCollector collector)
	{
		String word = input.getStringByField("word");
		Long count = input.getLongByField("count");
		
		System.out.println("[" + word + ", " + count + "]");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		
	}
}