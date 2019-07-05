package word_count_parallelized;

import java.util.List;
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

public class Splitter extends BaseRichBolt
{
	private OutputCollector collector;
	private List<Integer> counterTasks;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(new Fields("word", "end"));
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
	{
		this.collector = collector;
		this.counterTasks = context.getComponentTasks("counter"); 
	}

	@Override
	public void execute(Tuple input)
	{
		String data = input.getStringByField("sentence");
		boolean end = input.getBooleanByField("end");

		String[] t = data.split(" ");
		for (int i = 0; i < t.length; i++)
		{
			int emitTo = counterTasks.get(new String(t[i].toLowerCase()).charAt(0) - 97);
						
			if (end && i == t.length - 1)
				collector.emitDirect(emitTo, new Values(t[i].toLowerCase(), false));
			else
				collector.emitDirect(emitTo, new Values(t[i].toLowerCase(), false));
		}
		
		if (end)
			for (int i = 0 ; i < counterTasks.size(); i++)
				collector.emitDirect(counterTasks.get(i), new Values("", true));
	}
}