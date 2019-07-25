package compiler.interfaces;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

public abstract class Operator extends BaseBasicBolt implements Vertex
{
    protected static int id = 1;
    protected int vertexId = id++;

    @Override
    public String getName()
    {
        return "operator" + vertexId;
    }

    public abstract void execute(Tuple input, BasicOutputCollector collector);
    public abstract void declareOutputFields(OutputFieldsDeclarer declarer);
}