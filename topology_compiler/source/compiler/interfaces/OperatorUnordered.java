package compiler.interfaces;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.List;

public abstract class OperatorUnordered extends Operator
{
    public abstract List<Values> processElement(Tuple tuple);

    protected BasicOutputCollector collector;
    @Override
    public void execute(Tuple input, BasicOutputCollector collector)
    {
        this.collector = collector;
        List<Values> returned = processElement(input);

        if (returned == null)
            return;

        for(Values v : returned)
            collector.emit(v);
    }

    public abstract void declareOutputFields(OutputFieldsDeclarer declarer);
}