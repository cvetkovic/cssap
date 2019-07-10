import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.List;
import java.util.Map;

public class Splitter extends BaseRichBolt
{
    private OutputCollector collector;
    private List<Integer> counterTasks;
    private boolean shutdown = false;

    private int M, N;

    public Splitter(int M, int N)
    {
        this.M = M;
        this.N = N;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("word", "end"));
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
    {
        this.collector = collector;
        this.counterTasks = context.getComponentTasks("counter");
    }

    public void execute(Tuple input)
    {
        String data = input.getStringByField("payload");
        boolean end = input.getBooleanByField("endOfStream");

        if (end)
        {
            collector.emit(new Values(null, true));
            System.out.println("=====================> END RECEIVED BY SPLITTER <=====================");
        }
        else
        {
            String[] t = data.split(" ");
            for (int i = 0; i < t.length; i++)
                collector.emit(new Values(t[i].toLowerCase(), false));
        }
    }
}