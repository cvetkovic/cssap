package cvetkovic;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class Merger extends BaseBasicBolt
{
    private int expectingGroup = 0;
    private ImpatienceSortBuffer buffer;
    private long topologyStartupTime;

    private final long WARMUP_TIME = 240000; // 4 * 60 * 1000

    @Override
    public void prepare(Map stormConf, TopologyContext context)
    {
        super.prepare(stormConf, context);

        buffer = new ImpatienceSortBuffer();
        topologyStartupTime = System.currentTimeMillis();
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector)
    {
        double measurement = input.getDouble(0);
        int number = input.getInteger(1);
        long timestamp = input.getLong(2);
        int emitAllTo = input.getInteger(3);

        // standard addition to sort
        KV kv = new KV(measurement, number, timestamp);
        buffer.Insert(kv);

        if (emitAllTo != -1)
        {
            List<KV> result = buffer.GetAllKVLowerThan(emitAllTo);

            if (result == null)
                collector.emit(new Values(kv.getMeasurement()));

            for (KV elem : result)
            {
                if (System.currentTimeMillis() - topologyStartupTime >= WARMUP_TIME)
                    System.out.println((long)(System.currentTimeMillis() - elem.getTimestamp()));

                collector.emit(new Values(elem.getMeasurement()));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("data"));
    }
}