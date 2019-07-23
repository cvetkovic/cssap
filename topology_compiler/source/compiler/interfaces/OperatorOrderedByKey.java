package compiler.interfaces;

import compiler.structures.MinHeap;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.List;

public abstract class OperatorOrderedByKey extends Operator
{
    private MinHeap minHeap;
    private String keyFieldName;

    private int expecting = 0;

    public OperatorOrderedByKey(String keyFieldName)
    {
        this.keyFieldName = keyFieldName;

        this.minHeap = new MinHeap(10000, keyFieldName);
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector)
    {
        int orderNumber = input.getIntegerByField(keyFieldName);

        if (orderNumber == expecting)
        {
            List<Values> returned = processElement(input);

            if (returned == null)
                return;

            for(Values v : returned)
                collector.emit(v);

            expecting++;
        }
        else
        {
            minHeap.insert(input);
            return;
        }

        flushBuffer(collector);
    }

    private void flushBuffer(BasicOutputCollector collector)
    {
        while (minHeap.First() != null && minHeap.First().getIntegerByField(keyFieldName) == expecting)
        {
            Tuple tuple = minHeap.GetMinimum();
            List<Values> returned = processElement(tuple);

            for(Values v : returned)
                collector.emit(v);

            expecting++;
        }
    }

    public abstract void declareOutputFields(OutputFieldsDeclarer declarer);
    public abstract List<Values> processElement(Tuple tuple);
}