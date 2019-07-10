package cssap;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class Interpolation extends BaseBasicBolt
{
    private int expecting = 0;
    private MinHeap minHeap;

    public Interpolation()
    {
        minHeap = new MinHeap(1000);
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector)
    {
        double measurement = input.getDouble(0);
        int number = input.getInteger(1);

        try
        {
            minHeap.insert(measurement, number);

            if (number == expecting)
                flushBuffer(collector);
            else
                System.out.println("Waiting for element in row to come.");
        } catch (Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("sum"));
    }

    private double runningSum = 0;
    private int cnt = 0;

    private void flushBuffer(BasicOutputCollector collector) throws Exception
    {
        while (minHeap.First() != null && minHeap.First().getOrderNumber() == expecting)
        {
            KV kv = minHeap.GetMinimum();
            runningSum += kv.getMeasurement();
            cnt++;

            if (cnt % 10 == 0)
            {
                collector.emit(new Values(runningSum / cnt));
            }

            expecting++;
        }
    }
}