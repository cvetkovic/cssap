package amplitude_modulation;

import java.io.*;
import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class CarrierGenerator extends BaseRichSpout
{
	private static long id = 0; 
	private SpoutOutputCollector collector;
	
	private double amplitude, frequency, phase, sampling_frequency;	
	
	private static BufferedWriter writer;
	
	public CarrierGenerator(double sampling_frequency, double amplitude, double frequency, double phase)
	{
		this.sampling_frequency = sampling_frequency;
		this.amplitude = amplitude;
		this.frequency = frequency;
		this.phase = phase;
		
		try
		{
			writer = new BufferedWriter(new FileWriter("carrier.txt"));
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
		double t = id * (1 / sampling_frequency);
		double generatedValue = amplitude * Math.cos(2 * Math.PI * frequency * t + phase);
		
		collector.emit(new Values(generatedValue, SignalType.CARRIER, id));
		
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