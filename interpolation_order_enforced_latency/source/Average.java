package cvetkovic;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Date;

public class Average extends BaseBasicBolt
{
    private long lastSentAt = 0;
    private double runningSum = 0;
    private long numberOfItems = 0;

    private static final int INTERVAL = 1000;

    @Override
    public void execute(Tuple input, BasicOutputCollector collector)
    {
        runningSum += input.getDouble(0);
        numberOfItems++;

        long currentTime = new Date().getTime();
        if (currentTime - lastSentAt >= INTERVAL)
        {
            lastSentAt = currentTime;

            collector.emit(new Values(runningSum / numberOfItems));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("value"));
    }
}