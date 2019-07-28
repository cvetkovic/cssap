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
        {
            // calculating size of the final array of operators
            countOfNodes += pipelines[i].operators.length;

            // checking whether the type of last operator of current pipeline and of the next in the row are the same
            if (i != pipelines.length - 1)
            {
                int leftNumber = pipelines[i].operators.length - 1;

                if (!pipelines[i].operators[leftNumber].getInputClassType().equals(pipelines[i + 1].operators[0].getOutputClassType()))
                    throw new RuntimeException("Output type of the last operator in each pipeline has to match to the type of the following operator in the next pipeline.");
            }
        }

        operators = new Operator[countOfNodes];
        int i = 0;
        for (Pipeline p : pipelines)
            for (Operator o : p.operators)
                operators[i++] = o;
    }

    public void executeTopologyWithoutStorm(IProducer producer, IConsumer consumer)
    {
        producer.subscribe(operators[0]);
        producer.setCallback(new ICallback()
        {
            @Override
            public void callback(Object item)
            {
                operators[0].next(item);
            }
        });
        for (int i = 0; i < this.operators.length; i++)
        {
            if (i < this.operators.length - 1)
            {
                final int j = i;
                this.operators[i].subscribe(this.operators[i + 1]);
                this.operators[i].setCallback(new ICallback()
                {
                    @Override
                    public void callback(Object item)
                    {
                        operators[j + 1].next(item);
                    }
                });
            }
            else
            {
                this.operators[i].subscribe(consumer);
                this.operators[i].setCallback(new ICallback()
                {
                    @Override
                    public void callback(Object item)
                    {
                        consumer.next(item);
                    }
                });
            }
        }

        while (true)
        {
            producer.next();
        }
    }

    public StormTopology getStormTopology(IProducer producer, IConsumer consumer)
    {
        TopologyBuilder builder = new TopologyBuilder();

        // interconnection moved to generateSpout/Operator/Sink
        /*producer.subscribe(operators[0]);
        for (int i = 0; i < operators.length && operators.length > 1; i++)
            operators[i].subscribe(operators[i + 1]);
        operators[operators.length - 1].subscribe(consumer);*/

        BaseRichSpout source = generateSpout(producer, consumer);
        BaseBasicBolt[] operators = new BaseBasicBolt[this.operators.length];
        for (int i = 0; i < this.operators.length; i++)
            if (i < this.operators.length - 1)
                operators[i] = generateOperator(this.operators[i], this.operators[i + 1]);
            else
                operators[i] = generateOperator(this.operators[i], consumer);
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

    private BaseRichSpout generateSpout(IProducer producer, IConsumer consumer)
    {
        return new BaseRichSpout()
        {
            SpoutOutputCollector collector;
            ICallback callback;

            @Override
            public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
            {
                this.collector = collector;
                producer.subscribe(consumer);

                callback = new ICallback()
                {
                    @Override
                    public void callback(Object item)
                    {
                        collector.emit(new Values(item));
                    }
                };
                producer.setCallback(callback);
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

    private BaseBasicBolt generateOperator(Operator operator, IConsumer consumer)
    {
        return new BaseBasicBolt()
        {
            ICallback callback;

            @Override
            public void execute(Tuple input, BasicOutputCollector collector)
            {
                callback = new ICallback()
                {
                    @Override
                    public void callback(Object item)
                    {
                        collector.emit(new Values(item));
                    }
                };
                operator.setCallback(callback);

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