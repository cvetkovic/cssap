package amplitude_modulation;

import java.util.Map;

import org.apache.storm.shade.org.apache.commons.collections.map.HashedMap;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class Multiplier extends BaseBasicBolt
{
	private Map<Long, Double> buffer = new HashedMap();
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector)
	{
		long id = input.getLongByField("order_number");
		double value = input.getDoubleByField("value");
		
		if (buffer.containsKey(id))
		{
			double product = buffer.get(id) * value;
			collector.emit(new Values(id, product));
		}
		else
		{
			buffer.put(id, value);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(new Fields("order_number2", "value2"));
	}

}