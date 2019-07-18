package cvetkovic;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Date;
import java.util.Map;
import java.util.Random;

public class Generator extends BaseRichSpout
{
    private SpoutOutputCollector collector;
    private static int number = 0;
    private Random r = new Random();
    private Random punctuationGenerator = new Random();

    private int count = 0;
    private int numberOfEmittedTuples = 0;
    private int nextPunctuantion = -1;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
    {
        this.collector = collector;
        r.setSeed(new Date().getTime());

        nextPunctuantion += punctuationGenerator.nextInt(100);
    }

    @Override
    public void nextTuple()
    {
        if (++count != 10)
            return;

        double measurement = r.nextDouble() * 5;
        double randomThing = r.nextDouble();

        if (nextPunctuantion == numberOfEmittedTuples)
        {
            collector.emit(new Values(measurement, randomThing, number++, System.currentTimeMillis(), numberOfEmittedTuples));
            nextPunctuantion += punctuationGenerator.nextInt(100);
        }
        else
            collector.emit(new Values(measurement, randomThing, number++, System.currentTimeMillis(), -1));

        numberOfEmittedTuples++;
        count = 0;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("measurement", "payload", "orderNumber", "timestamp", "emitAllTo"));
    }
}