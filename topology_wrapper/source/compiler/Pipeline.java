package compiler;

import compiler.interfaces.*;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.TopologyBuilder;

public class Pipeline
{
    private TopologyBuilder builder;

    public Pipeline()
    {
        builder = new TopologyBuilder();
    }

    public void defineSource(Source source)
    {
        builder.setSpout(source.getName(), source);
    }

    public void defineOperator(Operator target, int parallelismHint, Vertex... sources)
    {
        if (target instanceof OperatorOrderedByKey && parallelismHint > 1)
            throw new RuntimeException("Parallelism is not allowed for ordered operator due to semantics preservation.");

        BoltDeclarer declarer = builder.setBolt(target.getName(), target, parallelismHint);
        for (int i = 0 ; i < sources.length; i++)
            declarer.shuffleGrouping(sources[i].getName());
    }

    public void defineSink(Sink printer, Vertex source)
    {
        builder.setBolt(printer.getName(), printer).shuffleGrouping(source.getName());
    }

    public StormTopology getCompiledStormTopology()
    {
        return builder.createTopology();
    }
}