package interpolation;

import compiler.interfaces.Source;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Date;
import java.util.Map;
import java.util.Random;

public class Generator extends Source
{
    private static int number = 0;
    private Random r = new Random();

    private int count = 0;

    /////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////// BaseRichSpout ///////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
    {
        this.collector = collector;

        r.setSeed(new Date().getTime());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("orderNumber", "measurement"));
    }

    /////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////// Source<Integer, Double> //////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////
    @Override
    public Values generateNext()
    {
        if (++count != 10)
            return null;

        double measurement = r.nextDouble() * 2.5;
        Values result = new Values(number++, measurement);
        count = 0;

        return result;
    }
}