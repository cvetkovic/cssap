import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class Counter extends BaseBasicBolt
{
    private Map<String, Long> data = new HashMap<>();
    private int endOfStreamsReceived = 0;
    private int expectedToEnd;

    private int M, N;

    public Counter(int M, int N)
    {
        this.M = M;
        this.expectedToEnd = M;
        this.N = N;
    }

    public void execute(Tuple input, BasicOutputCollector collector)
    {
        String word = input.getStringByField("word");
        boolean end = input.getBooleanByField("end");

        if (end)
        {
            endOfStreamsReceived++;

            if (endOfStreamsReceived == expectedToEnd)
            {
                data.forEach((k, v) -> collector.emit(new Values(k, v, false)));
                collector.emit(new Values(null, null, true));
                System.out.println("=====================> END RECEIVED BY COUNTER <=====================");
            }
        }
        else
        {
            if (data.containsKey(word))
                data.replace(word, data.get(word) + 1);
            else
                data.put(word, 1L);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("word", "count", "end"));
    }
}