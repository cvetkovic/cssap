package compiler.interfaces;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Values;

import java.util.Map;

public abstract class Source extends BaseRichSpout implements Vertex
{
    protected static int id = 1;
    protected int vertexId = id++;

    // my methods
    public abstract Values generateNext();

    @Override
    public String getName()
    {
        return "source" + vertexId;
    }

    // derived from BaseRichSpout
    protected SpoutOutputCollector collector;

    public abstract void open(Map conf, TopologyContext context, SpoutOutputCollector collector);

    public void nextTuple()
    {
        Values nextItem = generateNext();
        if (nextItem != null)
            collector.emit(nextItem);
    }

    public abstract void declareOutputFields(OutputFieldsDeclarer declarer);
}