package compiler;

import compiler.interfaces.AtomicOperator;
import compiler.interfaces.InfiniteSource;
import compiler.interfaces.ParallelComposition;
import compiler.interfaces.StreamComposition;
import compiler.interfaces.basic.IConsumer;
import compiler.interfaces.basic.Operator;
import compiler.storm.StormNode;
import compiler.storm.StormSink;
import compiler.storm.StormSource;
import compiler.storm.SystemMessage;
import compiler.storm.groupings.CopyGrouping;
import compiler.storm.groupings.RoundRobinSplitterGrouping;
import org.apache.storm.generated.Grouping;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
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
        // TODO: make sure that all atomicOperators names are distinct

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
            BoltDeclarer declarer = builder.setBolt(node.getOperator().getName(), node.getBolt(), node.getOperator().getParallelismHint());
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
                    node.getDeclarer().globalGrouping(s.getName());

            // linking operator themselves
            StormNode link = getNodeSubscribedTo(node.getOperator(), atomicOperators);
            doLinking(link, node.getDeclarer());
        }

        ////////////////////////////////////////////////////////////////////
        //              SINK GENERATION & LINKAGE
        ////////////////////////////////////////////////////////////////////
        for (int i = 0; i < sinks.length; i++)
        {
            StormSink ss = new StormSink(generateSink(sinks[i]));

            BoltDeclarer declarer = builder.setBolt(ss.getStormName(), ss.getBolt());

            StormNode link = getNodeSubscribedTo(sinks[i], atomicOperators);
            doLinking(link, declarer);
        }

        return builder.createTopology();
    }

    private StormNode getNodeSubscribedTo(IConsumer subscribedTo, List<StormNode> nodes)
    {
        for (StormNode n : nodes)
            for (int i = 0; i < n.getOperator().getConsumers().length; i++)
                if (n.getOperator().getConsumers()[i] == subscribedTo)
                    return n;

        return null;
    }

    private void doLinking(StormNode link, BoltDeclarer declarer)
    {
        if (link != null)
        {
            if (link.getOperator().getOutputArity() == 1)
                declarer.globalGrouping(link.getOperator().getName());
            else
            {
                // Custom grouping has to be implemented
                CustomStreamGrouping customGrouping;
                if (link.getCustomGrouping() == null)
                {
                    if (link.getOperator().getOperation() == Operator.Operation.COPY)
                        customGrouping = new CopyGrouping(link.getOperator().getName());
                    else if (link.getOperator().getOperation() == Operator.Operation.ROUND_ROBIN_SPLITTER)
                        customGrouping = new RoundRobinSplitterGrouping(link.getOperator().getName());
                    else
                        throw new RuntimeException("The provided topology cannot be compiled into a Storm topology because one of operators have not been yet implemented.");

                    link.setCustomGrouping(customGrouping);
                }
                else
                    customGrouping = link.getCustomGrouping();

                declarer.customGrouping(link.getOperator().getName(), customGrouping);
            }
        }
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

            private int i = 0;

            @Override
            public void nextTuple()
            {
                // TODO: remove these constraints (DEBUGGING PURPOSE ONLY)
                if (i < 8)
                    for (i = 0; i < 8; i++)
                        collector.emit(new Values(source.next(), null));
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer)
            {
                declarer.declare(new Fields("data", "message"));
            }
        };
    }

    private BaseBasicBolt generateOperator(AtomicOperator operator)
    {
        return new BaseBasicBolt()
        {
            private BasicOutputCollector collector;
            private IConsumer[] internalConsumers;

            private Integer[] taskIds;

            @Override
            public void execute(Tuple input, BasicOutputCollector collector)
            {
                this.collector = collector;
                Object item = input.getValueByField("data");

                // 0 is irrelevant here
                operator.next(0, item);
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer)
            {
                declarer.declare(new Fields("data", "message"));
            }

            @Override
            public void prepare(Map stormConf, TopologyContext context)
            {
                super.prepare(stormConf, context);

                Map<String, Grouping> connectedTo = context.getThisTargets().get("default");
                Iterator<String> nextOperatorNames = connectedTo.keySet().iterator();

                taskIds = new Integer[connectedTo.size()];
                int k = 0;
                while (nextOperatorNames.hasNext())
                    taskIds[k++] = context.getComponentTasks(nextOperatorNames.next()).get(0);

                internalConsumers = new IConsumer[operator.getOutputArity()];
                for (int i = 0; i < internalConsumers.length; i++)
                {
                    internalConsumers[i] = new IConsumer()
                    {
                        @Override
                        public int getInputArity()
                        {
                            throw new RuntimeException("This method was never supposed to be called.");
                        }

                        @Override
                        public void next(int channelNumber, Object item)
                        {
                            SystemMessage message = null;
                            if (operator.getOutputArity() > 1)
                            {
                                int taskGoingTo = taskIds[channelNumber];

                                if (operator.getOperation() == Operator.Operation.COPY)
                                    message = new SystemMessage(operator.getName(),
                                            operator.getOperation(),
                                            new SystemMessage.MeantFor(taskGoingTo));
                                else if (operator.getOperation() == Operator.Operation.ROUND_ROBIN_SPLITTER)
                                    message = new SystemMessage(operator.getName(),
                                            operator.getOperation(),
                                            new SystemMessage.MeantFor(taskGoingTo));
                                else
                                    throw new RuntimeException("Operator grouping has not been implemented.");
                            }
                            else
                                message = new SystemMessage();

                            collector.emit(new Values(item, message));
                        }
                    };
                }

                operator.subscribe(internalConsumers);
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

    public void linkSourceToOperator(String name, InfiniteSource source, Operator compositionFinal)
    {
        sources.add(new StormSource(name, source, NodesFactory.getMostLeftOperator(compositionFinal)));
    }
}