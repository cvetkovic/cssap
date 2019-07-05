package amplitude_modulation;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

public class OutputSignalWriter extends BaseBasicBolt
{
	private static BufferedWriter writer;
	
	public OutputSignalWriter()
	{
		try
		{
			writer = new BufferedWriter(new FileWriter("output.txt"));
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector)
	{
		long id = input.getLongByField("order_number2");
		double value = input.getDoubleByField("value2");
		
		try
		{
			writer.write("id: " + id + "; ");
			writer.write("value: " + value);
			writer.newLine();
			
			writer.flush();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}

	@Override
	public void cleanup()
	{
		super.cleanup();
		
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
		
	}

}