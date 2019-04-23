package word_count;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class Counter extends BaseBasicBolt
{
	private Map<String, Long> data = new HashMap<>();

	@Override
	public void execute(Tuple input, BasicOutputCollector collector)
	{
		String word = input.getStringByField("word");
		boolean end = input.getBooleanByField("end");

		if (data.containsKey(word))
			data.replace(word, data.get(word) + 1);
		else
			data.put(word, 1L);

		if (end)
			data.forEach((k, v) -> collector.emit(new Values(k, v)));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(new Fields("word", "count"));
	}
}