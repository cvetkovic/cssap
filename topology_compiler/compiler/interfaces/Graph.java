package compiler.interfaces;

import compiler.AtomicGraph;
import compiler.ParallelGraph;
import compiler.SerialGraph;
import compiler.interfaces.basic.Source;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public abstract class Graph
{
    protected String name;

    public abstract int getInputArity();

    public abstract int getOutputArity();

    public String getName()
    {
        return name;
    }

    public void executeLocal(Source source)
    {
        while (source.hasNext())
        {
            if (this instanceof AtomicGraph)
                ((AtomicGraph) this).getOperator().next(0, source.next());
            else if (this instanceof SerialGraph)
                ((SerialGraph) this).getGraphs()[0].getOperator().next(0, source.next());
            else if (this instanceof ParallelGraph)
            {
                ParallelGraph g = (ParallelGraph) this;

                if (g.getInputArity() == 1)
                    ((AtomicGraph) this).getOperator().next(0, source.next());
                else
                    throw new RuntimeException("Compiler doesn't know to which channel to send tuple.");
            }
        }
    }

    public StormTopology getStormTopology(Source source, AtomicGraph sink)
    {
        TopologyBuilder builder = new TopologyBuilder();
        BaseRichSpout spout = generateSpout(source);


        return builder.createTopology();
    }

    public BaseRichSpout generateSpout(Source source)
    {
        return new BaseRichSpout()
        {
            private SpoutOutputCollector collector;

            @Override
            public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
            {
                this.collector = collector;
            }

            @Override
            public void nextTuple()
            {
                collector.emit(new Values(source.next()));
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer)
            {
                declarer.declare(new Fields("data", "message"));
            }
        };
    }

    public BaseBasicBolt generateBolt(AtomicGraph graph)
    {
        return new BaseBasicBolt()
        {
            @Override
            public void prepare(Map stormConf, TopologyContext context)
            {
                super.prepare(stormConf, context);


            }

            @Override
            public void execute(Tuple input, BasicOutputCollector collector)
            {

            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer)
            {
                declarer.declare(new Fields("data", "message"));
            }
        };
    }

    public BaseBasicBolt generateSink(AtomicGraph sink)
    {
        if (sink.getOutputArity() != 0)
            throw new RuntimeException("Output arity of provided parameter must be zero.");

        return new BaseBasicBolt()
        {
            @Override
            public void execute(Tuple input, BasicOutputCollector collector)
            {
                Object item = input.getValueByField("message");
                sink.getOperator().next(0, item);
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer)
            {

            }
        };
    }
}