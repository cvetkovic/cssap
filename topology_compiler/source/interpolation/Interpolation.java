package interpolation;

import compiler.interfaces.OperatorOrderedByKey;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import java.util.LinkedList;
import java.util.List;

public class Interpolation extends OperatorOrderedByKey
{
    // for remembering last interpolation point
    private double previous = 0;

    public Interpolation(String keyFieldName)
    {
        super(keyFieldName);
    }

    @Override
    public List<Values> processElement(Tuple tuple)
    {
        double measurement = tuple.getDoubleByField("measurement");
        double interpolatedValue = (previous + measurement) / 2;
        previous = measurement;

        List<Values> result = new LinkedList<>();
        result.add(new Values(interpolatedValue));
        result.add(new Values(measurement));

        return result;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("data"));
    }
}