package cssap;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class Printer extends BaseBasicBolt
{
    @Override
    public void execute(Tuple input, BasicOutputCollector collector)
    {
        double avg = input.getDouble(0);
        System.out.println(avg);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {

    }
}