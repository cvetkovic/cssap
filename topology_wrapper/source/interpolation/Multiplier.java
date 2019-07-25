package interpolation;

import compiler.interfaces.OperatorUnordered;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Collections;
import java.util.List;
import java.util.Random;

public class Multiplier extends OperatorUnordered
{
    private Random r = new Random();

    public Multiplier()
    {
        r.setSeed(System.currentTimeMillis());
    }

    @Override
    public List<Values> processElement(Tuple tuple)
    {
        int orderNumber = tuple.getInteger(0);
        double measurement = tuple.getDouble(1);

        try
        {
            Thread.sleep((long)(r.nextDouble() * 10));
        }
        catch (Exception ex)
        {
            throw new RuntimeException("SLEEP INTERRUPTED IN Multiplier.java");
        }

        return Collections.singletonList(new Values(orderNumber, 2 * measurement));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("orderNumber", "measurement"));
    }
}