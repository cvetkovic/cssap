package compiler;

import compiler.interfaces.*;
import compiler.structures.KV;
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

import java.io.Serializable;
import java.util.Map;

public class Pipeline implements Serializable
{
    private IProducer source;
    private Operator[] operators;
    private IConsumer sink;

    public Pipeline(IProducer source, IConsumer sink, Operator... consumers)
    {
        this.source = source;
        this.sink = sink;
        operators = new Operator[consumers.length];

        for (int i = 0; i < consumers.length; i++)
            operators[i] = consumers[i];

        // producer <--> consumer subscription
        source.subscribe(operators[0]);
        for (int i = 0; i < operators.length; i++)
            if (i == operators.length - 1)
                operators[i].subscribe(sink);
            else
                operators[i].subscribe(operators[i + 1]);
    }

    public void executeTopologyWithoutStorm()
    {
        while (true)
        {
            KV<Integer, Double> subresult = (KV) source.next(null);
            for (int i = 0; i < this.operators.length; i++)
                subresult = (KV) this.operators[i].next(subresult);
            sink.next(subresult);
        }
    }

    public StormTopology getStormTopology()
    {
        TopologyBuilder builder = new TopologyBuilder();

        BaseRichSpout source = generateSpout(this.source);
        BaseBasicBolt[] operators = new BaseBasicBolt[this.operators.length];
        for (int i = 0; i < this.operators.length; i++)
            operators[i] = generateOperator(this.operators[i]);
        BaseBasicBolt sink = generateSink(this.sink);

        ///////////////////////////////////////////////////////////////////////////////////////////////
        ////////////////////////////// CONNECTING THE GENERATED TOPOLOGY //////////////////////////////
        ///////////////////////////////////////////////////////////////////////////////////////////////
        int name = 1;

        builder.setSpout("operator" + (name++), source);
        for (int i = 0; i < this.operators.length; i++)
        {
            builder.setBolt("operator" + name, operators[i]).shuffleGrouping("operator" + (name - 1));
            name++;
        }
        builder.setBolt("operator" + name, sink).shuffleGrouping("operator" + (name - 1));
        ///////////////////////////////////////////////////////////////////////////////////////////////
        ///////////////////////////////////////////////////////////////////////////////////////////////
        ///////////////////////////////////////////////////////////////////////////////////////////////

        return builder.createTopology();
    }

    private BaseRichSpout generateSpout(IProducer producer)
    {
        return new BaseRichSpout()
        {
            SpoutOutputCollector collector;

            @Override
            public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
            {
                this.collector = collector;
            }

            @Override
            public void nextTuple()
            {
                KV<Integer, Double> kv = (KV<Integer, Double>) producer.next(null);
                if (kv != null)
                    collector.emit(new Values(kv.getK(), kv.getV()));
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer)
            {
                declarer.declare(new Fields("field1", "field2"));
            }
        };
    }

    private BaseBasicBolt generateOperator(Operator operator)
    {
        return new BaseBasicBolt()
        {
            @Override
            public void execute(Tuple input, BasicOutputCollector collector)
            {
                KV kv = new KV(input.getInteger(0), input.getDouble(1));
                kv = (KV<Integer, Double>) operator.next(kv);
                if (kv != null)
                    collector.emit(new Values(kv.getK(), kv.getV()));
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer)
            {
                declarer.declare(new Fields("field1", "field2"));
            }

            @Override
            public void prepare(Map stormConf, TopologyContext context)
            {
                super.prepare(stormConf, context);
            }
        };
    }

    private BaseBasicBolt generateSink(IConsumer consumer)
    {
        return new BaseBasicBolt()
        {
            @Override
            public void execute(Tuple input, BasicOutputCollector collector)
            {
                KV kv = new KV(input.getInteger(0), input.getDouble(1));
                if (kv != null)
                    consumer.next(kv);
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer)
            {
                declarer.declare(new Fields("field1", "field2"));
            }

            @Override
            public void prepare(Map stormConf, TopologyContext context)
            {
                super.prepare(stormConf, context);
            }
        };
    }
}