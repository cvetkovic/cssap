package compiler;

import compiler.interfaces.AtomicOperator;
import compiler.interfaces.ParallelComposition;
import compiler.interfaces.StreamComposition;
import compiler.interfaces.basic.IConsumer;
import compiler.interfaces.basic.InfiniteSource;
import compiler.interfaces.basic.Operator;
import compiler.storm.StormNode;
import compiler.storm.StormSink;
import compiler.storm.StormSource;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.*;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.Serializable;
import java.util.*;

public class Graph implements Serializable
{
    private List<StormSource> sources = new LinkedList<>();
    private final Operator[] graphOperators;
    private IConsumer[] sinks;

    public Graph(Operator... consumers)
    {
        graphOperators = new Operator[consumers.length];

        for (int i = 0; i < consumers.length; i++)
            graphOperators[i] = consumers[i];
    }

    /*public void executeTopologyWithoutStorm(Function0 code, IProducer producer, IConsumer consumer)
    {
        producer.subscribe(1, operators[0]);

        for (int i = 0; i < this.operators.length; i++)
            if (i < this.operators.length - 1)
                this.operators[i].subscribe(1, this.operators[i + 1]);
            else
                this.operators[i].subscribe(1, consumer);

        while (true)
            producer.next(1, code.call());
    }*/

    private List<AtomicOperator> resolveComposition(Operator operator)
    {
        if (operator instanceof AtomicOperator)
            return Collections.singletonList((AtomicOperator) operator);
        else if (operator instanceof StreamComposition)
        {
            List<AtomicOperator> operators = new LinkedList<>();
            StreamComposition streamComposition = (StreamComposition) operator;

            Operator[] bricks = streamComposition.getConsistedOf();
            for (int i = 0; i < bricks.length; i++)
            {
                if (bricks[i] instanceof AtomicOperator)
                    operators.add((AtomicOperator) bricks[i]);
                else if (bricks[i] instanceof StreamComposition)
                    operators.addAll(resolveComposition(bricks[i]));
                else if (bricks[i] instanceof ParallelComposition)
                    // TODO: think about resolving parallel composition
                    throw new RuntimeException("Think about this.");
            }

            return operators;
        }
        else
        {
            List<AtomicOperator> operators = new LinkedList<>();
            ParallelComposition parallelComposition = (ParallelComposition) operator;

            Operator[] bricks = parallelComposition.getConsistedOf();
            for (int i = 0; i < bricks.length; i++)
            {
                if (bricks[i] instanceof AtomicOperator)
                    operators.add((AtomicOperator) bricks[i]);
                else
                    operators.addAll(resolveComposition(bricks[i]));
            }

            return operators;
        }
    }

    private List<StormNode> extractAtomicOperators()
    {
        List<StormNode> atomicOperators = new LinkedList<>();

        for (int i = 0; i < this.graphOperators.length; i++)
        {
            if (this.graphOperators[i] instanceof AtomicOperator)
                atomicOperators.add(new StormNode(this.graphOperators[i], generateOperator((AtomicOperator) this.graphOperators[i])));
            else if (this.graphOperators[i] instanceof StreamComposition)
            {
                List<AtomicOperator> operatorList = resolveComposition(this.graphOperators[i]);
                for (AtomicOperator operator : operatorList)
                    atomicOperators.add(new StormNode(operator, generateOperator(operator)));
            }
            else if (this.graphOperators[i] instanceof ParallelComposition)
            {
                List<AtomicOperator> operatorList = resolveComposition(this.graphOperators[i]);
                for (AtomicOperator operator : operatorList)
                    atomicOperators.add(new StormNode(operator, generateOperator(operator)));
            }
            else
                throw new RuntimeException("Provided type of operator is not implemented.");
        }

        return atomicOperators;
    }

    public StormTopology getStormTopology()
    {
        TopologyBuilder builder = new TopologyBuilder();
        // list of bolts made from atomic operators extracted from the topology
        List<StormNode> atomicOperators = extractAtomicOperators();

        ////////////////////////////////////////////////////////////////////
        //              STORM SPOUT CREATION
        ////////////////////////////////////////////////////////////////////
        for (int i = 0; i < sources.size(); i++)
        {
            BaseRichSpout spout = generateSpout(sources.get(i).getSource());
            sources.get(i).setSpout(spout);

            builder.setSpout(sources.get(i).getName(), spout);
        }

        ////////////////////////////////////////////////////////////////////
        //              STORM BOLT CREATION
        ////////////////////////////////////////////////////////////////////
        for (StormNode node : atomicOperators)
        {
            BoltDeclarer declarer = builder.setBolt(node.getStormId(), node.getBolt(), node.getOperator().getParallelismHint());
            node.setDeclarer(declarer);
        }

        ////////////////////////////////////////////////////////////////////
        //              LINKING OPERATORS
        ////////////////////////////////////////////////////////////////////
        for (StormNode node : atomicOperators)
        {
            // check if any source is connected to this operator and if yes link them
            for (StormSource s : sources)
                if (s.getConsumer() == node.getOperator())
                    node.getDeclarer().shuffleGrouping(s.getName());

            // linking operator themselves
            StormNode link = getNodeSubscribedTo(node.getOperator(), atomicOperators);
            if (link != null)
            {
                node.getDeclarer().shuffleGrouping(link.getStormId());
            }
        }

        ////////////////////////////////////////////////////////////////////
        //              SINK GENERATION & LINKAGE
        ////////////////////////////////////////////////////////////////////
        Map<IConsumer, StormSink> stormSinks = new HashMap<>();

        for (int i = 0; i < sinks.length; i++)
        {
            StormSink ss = new StormSink(generateSink(sinks[i]));

            BoltDeclarer declarer = builder.setBolt(ss.getStormName(), ss.getBolt());
            stormSinks.put(sinks[i], ss);

            for (StormNode n : atomicOperators)
                if (n.getOperator().getMapOfConsumers().containsValue(sinks[i]))
                    declarer.shuffleGrouping(n.getStormId());
        }

        return builder.createTopology();
    }

    private static StormNode getNodeSubscribedTo(Operator subscribedTo, List<StormNode> nodes)
    {
        for (StormNode n : nodes)
            if (n.getOperator().getMapOfConsumers().containsValue(subscribedTo))
                return n;

        return null;
    }

    private BaseRichSpout generateSpout(InfiniteSource source)
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
                declarer.declare(new Fields("data"));
            }
        };
    }

    private BaseBasicBolt generateOperator(AtomicOperator operator)
    {
        return new BaseBasicBolt()
        {
            private BasicOutputCollector collector;
            private IConsumer consumer = new IConsumer()
            {
                @Override
                public int getInputArity()
                {
                    return 1;
                }

                @Override
                public void next(int channelNumber, Object item)
                {
                    collector.emit(new Values(item));
                }
            };

            @Override
            public void execute(Tuple input, BasicOutputCollector collector)
            {
                /* this was put here to avoid cloning the whole topology
                 * because we cannot do clearSubscription because the
                 * deleted data would be used for compilation. Possible
                 * to do with two-pass compilation also
                 */
                if (!operator.getMapOfConsumers().containsValue(consumer))
                {
                    operator.clearSubscription();
                    operator.subscribe(consumer);
                }

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

    public void setSinks(IConsumer... sinks)
    {
        this.sinks = sinks.clone();
    }

    public void linkSourceToOperator(InfiniteSource source, Operator compositionFinal)
    {
        sources.add(new StormSource(source, NodesFactory.getMostLeftOperator(compositionFinal)));
    }
}