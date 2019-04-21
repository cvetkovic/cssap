package example1;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

public class RunningSumBolt extends BaseBasicBolt
{
	Map<Integer, Double> runningSum = new HashMap<Integer, Double>();
	
	public void execute(Tuple input, BasicOutputCollector collector)
	{
		int id = input.getInteger(0);
		double oldVal;
		if (runningSum.containsKey(id))
			oldVal = (double)runningSum.get(id);
		else
			oldVal = 0;
		double t = 0;
		
		if (runningSum.containsKey(id))
			runningSum.replace(id, oldVal, t = (oldVal + input.getDouble(1)));
		else
			runningSum.put(id, t);
		
		collector.emit(new Values(id, t));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		// TODO Auto-generated method stub
		declarer.declare(new Fields("id3", "value3"));
	}
}