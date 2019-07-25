package interpolation;

import compiler.interfaces.OperatorUnordered;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Collections;
import java.util.Date;
import java.util.List;

public class Average extends OperatorUnordered
{
    private long lastSentAt = 0;
    private double runningSum = 0;
    private long numberOfItems = 0;

    private static final int INTERVAL = 1000;

    @Override
    public List<Values> processElement(Tuple tuple)
    {
        runningSum += tuple.getDouble(0);
        numberOfItems++;

        long currentTime = new Date().getTime();
        if (currentTime - lastSentAt >= INTERVAL)
        {
            lastSentAt = currentTime;
            double result = runningSum / (double)numberOfItems;

            return Collections.singletonList(new Values(result));
        }

        return null;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("value"));
    }
}