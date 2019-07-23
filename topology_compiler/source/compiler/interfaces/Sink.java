package compiler.interfaces;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

public abstract class Sink extends BaseBasicBolt implements Vertex
{
    protected static int id = 1;
    protected int vertexId = id++;

    // my methods
    public abstract void processElement(Tuple tuple);

    @Override
    public String getName()
    {
        return "sink" + vertexId;
    }

    // derived from BaseRichSpout
    @Override
    public void execute(Tuple input, BasicOutputCollector collector)
    {
        processElement(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        // empty method because sink doesn't produce anything
    }
}