package compiler;

import compiler.interfaces.IConsumer;
import compiler.interfaces.IProducer;
import compiler.interfaces.Operator;
import compiler.interfaces.lambda.Function0;
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
    private final Operator[] operators;

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

    public void executeTopologyWithoutStorm(Function0 code, IProducer producer, IConsumer consumer)
    {
        producer.subscribe(1, operators[0]);

        for (int i = 0; i < this.operators.length; i++)
            if (i < this.operators.length - 1)
                this.operators[i].subscribe(1, this.operators[i + 1]);
            else
                this.operators[i].subscribe(1, consumer);

        while (true)
            producer.next(1, code.call());
    }

    public StormTopology getStormTopology(Function0 code, IProducer producer, IConsumer consumer)
    {
        TopologyBuilder builder = new TopologyBuilder();

        BaseRichSpout source = generateSpout(code, producer);
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

    private BaseRichSpout generateSpout(Function0 code, IProducer producer)
    {
        return new BaseRichSpout()
        {
            private SpoutOutputCollector collector;
            private IConsumer consumer = new IConsumer()
            {
                @Override
                public void next(int channelNumber, Object item)
                {
                    collector.emit(new Values(item));
                }
            };

            @Override
            public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
            {
                this.collector = collector;
                producer.subscribe(1, consumer);
            }

            @Override
            public void nextTuple()
            {
                producer.next(1, code.call());
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
            private IConsumer consumer = new IConsumer()
            {
                @Override
                public void next(int channelNumber, Object item)
                {
                    collector.emit(new Values(item));
                }
            };

            @Override
            public void execute(Tuple input, BasicOutputCollector collector)
            {
                this.collector = collector;
                Object item = input.getValueByField("data");

                operator.next(1, item);
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
                operator.subscribe(1, consumer);
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
                consumer.next(1, item);
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