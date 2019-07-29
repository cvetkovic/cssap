package compiler;

import compiler.interfaces.*;
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
import java.lang.reflect.Type;
import java.util.Map;

public class Pipeline implements Serializable
{
    private Operator[] operators;

    public Pipeline(Operator... consumers)
    {
        operators = new Operator[consumers.length];

        for (int i = 0; i < consumers.length; i++)
            operators[i] = consumers[i];
    }

    public Pipeline(Pipeline... pipelines)
    {
        int countOfNodes = 0;
        for (int i = 0; i < pipelines.length; i++)
            countOfNodes += pipelines[i].operators.length;

        operators = new Operator[countOfNodes];
        int i = 0;
        for (Pipeline p : pipelines)
            for (Operator o : p.operators)
                operators[i++] = o;
    }

    public void executeTopologyWithoutStorm(IProducer producer, IConsumer consumer)
    {
        producer.subscribe(operators[0]);

        for (int i = 0; i < this.operators.length; i++)
            if (i < this.operators.length - 1)
                this.operators[i].subscribe(this.operators[i + 1]);
            else
                this.operators[i].subscribe(consumer);

        while (true)
            producer.next();
    }

    public StormTopology getStormTopology(IProducer producer, IConsumer consumer)
    {
        TopologyBuilder builder = new TopologyBuilder();

        BaseRichSpout source = generateSpout(producer);
        BaseBasicBolt[] operators = new BaseBasicBolt[this.operators.length];
        for (int i = 0; i < this.operators.length; i++)
            operators[i] = generateOperator(this.operators[i]);
        BaseBasicBolt sink = generateSink(consumer);

        ///////////////////////////////////////////////////////////////////////////////////////////////
        ////////////////////////////// CONNECTING THE GENERATED TOPOLOGY //////////////////////////////
        ///////////////////////////////////////////////////////////////////////////////////////////////
        int name = 1;

        builder.setSpout("operator" + (name++), source);
        for (int i = 0; i < this.operators.length; i++)
        {
            builder.setBolt("operator" + name,
                    operators[i],
                    this.operators[i].getParallelismHint()).shuffleGrouping("operator" + (name - 1));
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
            private SpoutOutputCollector collector;
            private IConsumer consumer = item -> collector.emit(new Values(item));

            @Override
            public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
            {
                this.collector = collector;
                producer.subscribe(consumer);
            }

            @Override
            public void nextTuple()
            {
                producer.next();
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer)
            {
                declarer.declare(new Fields("data"));
            }
        };
    }

    private BaseBasicBolt generateOperator(Operator operator)
    {
        return new BaseBasicBolt()
        {
            private BasicOutputCollector collector;
            private IConsumer consumer = item -> collector.emit(new Values(item));

            @Override
            public void execute(Tuple input, BasicOutputCollector collector)
            {
                this.collector = collector;
                Object item = input.getValueByField("data");

                operator.next(item);
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer)
            {
                declarer.declare(new Fields("data"));
            }

            @Override
            public void prepare(Map stormConf, TopologyContext context)
            {
                super.prepare(stormConf, context);
                operator.subscribe(consumer);
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
                Object item = input.getValueByField("data");
                consumer.next(item);
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer)
            {
                declarer.declare(new Fields("data"));
            }

            @Override
            public void prepare(Map stormConf, TopologyContext context)
            {
                super.prepare(stormConf, context);
            }
        };
    }
}