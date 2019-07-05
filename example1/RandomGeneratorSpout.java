package example1;

import java.util.*;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class RandomGeneratorSpout extends BaseRichSpout
{
	private SpoutOutputCollector outputCollector;

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
	{
		outputCollector = collector;
	}

	public void nextTuple()
	{
		Utils.sleep((int)(Math.random() * 100));
		int id = (int)(Math.random() * 10);
		double val = Math.random() * 100 - 50;
		
		outputCollector.emit(new Values(id, val));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(new Fields("id1", "value1"));
	}

}
