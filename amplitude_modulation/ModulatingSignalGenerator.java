package amplitude_modulation;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class ModulatingSignalGenerator extends BaseRichSpout
{
	private static long id = 0; 
	private SpoutOutputCollector collector;
	
	private double min, max, sampling_frequency;
	private Random random;

	private static BufferedWriter writer;
	
	public ModulatingSignalGenerator(double sampling_frequency, double min, double max)
	{
		if (min > max)
			throw new IllegalArgumentException("Modulating signal constructor error -> min > max");
		
		this.sampling_frequency = sampling_frequency;
		this.min = min;
		this.max = max;
		
		this.random = new Random(System.currentTimeMillis());
		
		try
		{
			writer = new BufferedWriter(new FileWriter("modulating_signal.txt"));
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}


	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
	{
		this.collector = collector;
	}
	
	@Override
	public void nextTuple()
	{
		// order number of sample to be generated 
		double t = id * sampling_frequency;
		double generatedValue = random.nextDouble() * (max - min) + min;
		
		collector.emit(new Values(generatedValue, SignalType.SIGNAL, id));

		try
		{
			writer.write("id: " + id + "; ");
			writer.write("value: " + generatedValue);
			writer.newLine();
			
			writer.flush();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		
		id++;
		Utils.sleep(50);
	}

	@Override
	public void close()
	{
		super.close();
		
		try
		{
			writer.flush();
			writer.close();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(new Fields("value", "type", "order_number"));
	}
}