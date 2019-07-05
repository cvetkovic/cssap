package word_count;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class Splitter extends BaseBasicBolt
{
	private OutputCollector collector;
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector)
	{
		String data = input.getStringByField("sentence");
		boolean end = input.getBooleanByField("end");
		
		String[] t = data.split(" ");
		for (int i = 0; i < t.length; i++)
			if (end && i == t.length - 1)
				collector.emit(new Values(t[i].toLowerCase(), true));
			else
				collector.emit(new Values(t[i].toLowerCase(), false));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(new Fields("word", "end"));
	}
}