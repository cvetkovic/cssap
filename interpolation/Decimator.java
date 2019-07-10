package cssap;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Date;
import java.util.Random;

public class Decimator extends BaseBasicBolt
{
    private Random random = new Random();

    public Decimator()
    {
        random.setSeed(new Date().getTime());
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector)
    {
        double measurement = input.getDouble(0);
        int orderNumber = input.getInteger(2);

        collector.emit(new Values(measurement, orderNumber));

        try
        {
            Thread.sleep((long)(random.nextDouble() * 50.0));
        }
        catch (Exception ex)
        {

        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("measurement", "orderNumber"));
    }
}