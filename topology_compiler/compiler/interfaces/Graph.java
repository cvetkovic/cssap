package compiler.interfaces;

import compiler.AtomicGraph;
import compiler.SerialGraph;
import compiler.interfaces.basic.IConsumer;
import compiler.interfaces.basic.Operator;
import compiler.interfaces.basic.Source;
import compiler.storm.StormBolt;
import compiler.storm.SystemMessage;
import compiler.storm.groupings.MultipleOutputGrouping;
import compiler.structures.KV;
import compiler.structures.MinHeap;
import compiler.structures.SuccessiveNumberGenerator;
import org.apache.storm.generated.Grouping;
import org.apache.storm.generated.StormTopology;
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public abstract class Graph implements Serializable
{
    private static int uniqueId = 0;
    protected String name;

    public abstract int getInputArity();

    public abstract int getOutputArity();

    public abstract Operator getOperator();

    public String getName()
    {
        return name;
    }

    public void executeLocal(Source source)
    {
        while (source.hasNext())
            getOperator().next(0, source.next());
    }


    public StormTopology getStormTopology(Source source)
    {
        Map<IConsumer, StormBolt> cache = new HashMap<>();
        TopologyBuilder builder = new TopologyBuilder();
        BaseRichSpout spout = generateSpout(source);
        builder.setSpout("source", spout);

        Operator operator;
        if (this instanceof AtomicGraph)
            operator = getOperator();
        else if (this instanceof SerialGraph)
            operator = ((SerialGraph) this).getConstituentGraphs()[0];
        else
            throw new RuntimeException("Parallel graph not allowed on the beginning of the topology, because that implies that the topology would have multiple sources, which is not allowed.");

        BaseBasicBolt generated = generateOperator(new AtomicGraph(operator));
        BoltDeclarer declarer = builder.setBolt(operator.getName(), generated).globalGrouping("source");
        StormBolt previous = new StormBolt(operator, generated, declarer);
        cache.put(operator, previous);

        linkOperators(cache, builder, previous, operator);

        return builder.createTopology();
    }

    private void linkOperators(Map<IConsumer, StormBolt> cache, TopologyBuilder builder, StormBolt previous, Operator previousOperator)
    {
        for (int i = 0; i < previousOperator.getOutputArity(); i++)
        {
            IConsumer[] successors = previousOperator.getConsumers();

            ///////////////////////////////////////////////////////////////
            //  PARALLEL COMPOSITION CONSTITUENTS EXTRACTION
            ///////////////////////////////////////////////////////////////
            if (successors[i] instanceof Operator && ((Operator) successors[i]).getParallelConstituent() != null)
                successors = ((Operator) successors[i]).getParallelConstituent();

            if (successors[i] instanceof Operator)
            {
                StormBolt bolt;

                if (cache.get(successors[i]) == null)
                {
                    bolt = new StormBolt(successors[i],
                            generateOperator(new AtomicGraph((Operator) successors[i])),
                            null);

                    cache.put(successors[i], bolt);
                }
                else
                    bolt = cache.get(successors[i]);

                if (previousOperator.getOutputArity() == 1)
                {
                    // define bolt only once
                    if (bolt.getDeclarer() == null)
                    {
                        BoltDeclarer declarer = builder.setBolt(((Operator) bolt.getOperator()).getName(), bolt.getBolt()).globalGrouping(previousOperator.getName());
                        bolt.setDeclarer(declarer);
                    }
                    else
                    {
                        // operator has already been declared, so this means that its input arity is greater than one
                        bolt.getDeclarer().globalGrouping(previousOperator.getName());
                    }
                }
                else
                {
                    MultipleOutputGrouping customGrouping;

                    ///////////////////////////////////////////////////////////////
                    //  CREATING SHARED CUSTOM GROUPING CLASS
                    ///////////////////////////////////////////////////////////////
                    if (bolt.getCustomGrouping() == null)
                    {
                        customGrouping = new MultipleOutputGrouping(((Operator) previous.getOperator()).getName());
                        bolt.setCustomGrouping(customGrouping);
                    }
                    else
                    {
                        bolt.setCustomGrouping(customGrouping = bolt.getCustomGrouping());
                    }
                    ///////////////////////////////////////////////////////////////


                    ///////////////////////////////////////////////////////////////
                    //  LINKING OPERATORS
                    ///////////////////////////////////////////////////////////////
                    if (bolt.getDeclarer() == null)
                    {
                        BoltDeclarer declarer = builder.setBolt(((Operator) bolt.getOperator()).getName(),
                                bolt.getBolt()).customGrouping(((Operator) previous.getOperator()).getName(), customGrouping);
                        bolt.setDeclarer(declarer);
                    }
                    else
                        bolt.getDeclarer().customGrouping(((Operator) previous.getOperator()).getName(), customGrouping);
                }

                linkOperators(cache, builder, bolt, ((Operator) bolt.getOperator()));
            }
            else if (previousOperator.getConsumers()[i] instanceof IConsumer)
            {
                BaseBasicBolt generated = generateSink(previousOperator.getConsumers()[i]);
                builder.setBolt("sink" + Integer.toString(uniqueId++), generated).globalGrouping(previousOperator.getName());
            }
        }

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

            private int i = 0;

            @Override
            public void nextTuple()
            {
                SystemMessage message = new SystemMessage();
                message.addPayload(new SystemMessage.InputChannelSpecification(0));

                // TODO: remove counter <<i>> on deploy (DEBUG only)
                if (i++ == 10)
                {
                    collector.emit(new Values(source.next(), message));
                    i = 0;
                }
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer)
            {
                declarer.declare(new Fields("data", "message"));
            }
        };
    }

    private BaseBasicBolt generateOperator(AtomicGraph operator)
    {
        return new BaseBasicBolt()
        {
            private BasicOutputCollector collector;
            private IConsumer[] internalConsumers;

            private Integer[] taskIds;

            private SuccessiveNumberGenerator expectingSequence;
            private MinHeap buffer;
            private SystemMessage systemMessageBuffer;

            // common generator for whole operator
            private SuccessiveNumberGenerator sequenceNumberGenerator = null;

            @Override
            public void execute(Tuple input, BasicOutputCollector collector)
            {
                this.collector = collector;

                Object item = input.getValueByField("data");
                SystemMessage message = (SystemMessage) input.getValueByField("message");
                SystemMessage.Payload payload = message.getPayloadByType(SystemMessage.MessageTypes.INPUT_CHANNEL);

                ///////////////////////////////////////////////////////////////
                //  ORDER SHOULD BE PRESERVED IF TRUE
                ///////////////////////////////////////////////////////////////
                if (operator.getInputArity() > 1)
                {
                    if (expectingSequence == null)
                    {
                        expectingSequence = new SuccessiveNumberGenerator();
                        buffer = new MinHeap();
                    }

                    SystemMessage.SequenceNumber sequenceNumber = ((SystemMessage.SequenceNumber) message.getPayloadByType(SystemMessage.MessageTypes.SEQUENCE_NUMBER)).getSequenceNumberLeaf();
                    buffer.insert(new KV<Integer, Tuple>(sequenceNumber.sequenceNumber, input));

                    if (sequenceNumber.sequenceNumber == expectingSequence.getCurrentState() &&
                        message.getPayloadByType(SystemMessage.MessageTypes.END_OF_OUTPUT) != null)
                    {
                        ///////////////////////////////////////////////////////////////
                        //  CONSUME EVERYTHING THAT IS IN BUFFER AND IS IN RIGHT ORDER
                        ///////////////////////////////////////////////////////////////
                        message.deletePayloadFromMessage(SystemMessage.MessageTypes.END_OF_OUTPUT);
                        int lastSentToChannel = -1;

                        while (buffer.peek() != null && buffer.peek().getK() == expectingSequence.getCurrentState())
                        {
                            Tuple itemToSend = buffer.poll().getV();
                            expectingSequence.increase();

                            // TODO: remove consumed sequence number for sending it further

                            int inputChannel = ((SystemMessage.InputChannelSpecification) payload).inputChannel;
                            if (inputChannel != lastSentToChannel && lastSentToChannel != -1)
                                message.addPayload(new SystemMessage.EndOfOutput());
                            systemMessageBuffer = message;
                            operator.getOperator().next(inputChannel, itemToSend);
                            lastSentToChannel = inputChannel;
                        }
                    }
                }
                else
                {
                    systemMessageBuffer = message;
                    operator.getOperator().next(((SystemMessage.InputChannelSpecification) payload).inputChannel, item);
                    message.addPayload(new SystemMessage.EndOfOutput());
                }
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
                            SystemMessage message;
                            if (systemMessageBuffer == null)
                                message = new SystemMessage();
                            else
                                message = systemMessageBuffer;

                            if (operator.getOutputArity() > 1)
                            {
                                ///////////////////////////////////////////////////////////////
                                //  MEANT FOR
                                ///////////////////////////////////////////////////////////////
                                int taskGoingTo = taskIds[channelNumber];
                                message.addPayload(new SystemMessage.MeantFor(operator.getOperator().getName(), taskGoingTo));

                                ///////////////////////////////////////////////////////////////
                                //  SEQUENCE NUMBERS
                                ///////////////////////////////////////////////////////////////
                                if (sequenceNumberGenerator == null)
                                    sequenceNumberGenerator = new SuccessiveNumberGenerator();

                                SystemMessage.SequenceNumber newSn = new SystemMessage.SequenceNumber(sequenceNumberGenerator.next());
                                if (message.getPayloadByType(SystemMessage.MessageTypes.SEQUENCE_NUMBER) == null)
                                    message.addPayload(newSn);
                                else
                                    ((SystemMessage.SequenceNumber) message.getPayloadByType(SystemMessage.MessageTypes.SEQUENCE_NUMBER)).getSequenceNumberLeaf().assignSubsequence(newSn);
                            }
                            ///////////////////////////////////////////////////////////////
                            //  INPUT CHANNEL ROUTING
                            ///////////////////////////////////////////////////////////////
                            message.addPayload(new SystemMessage.InputChannelSpecification(channelNumber));

                            collector.emit(new Values(item, message));
                        }
                    };
                }

                operator.getOperator().subscribe(internalConsumers);
            }
        };
    }

    public BaseBasicBolt generateSink(IConsumer sink)
    {
        return new BaseBasicBolt()
        {
            @Override
            public void execute(Tuple input, BasicOutputCollector collector)
            {
                Object item = input.getValueByField("data");
                SystemMessage message = (SystemMessage) input.getValueByField("message");
                SystemMessage.Payload payload = message.getPayloadByType(SystemMessage.MessageTypes.INPUT_CHANNEL);

                sink.next(((SystemMessage.InputChannelSpecification) payload).inputChannel, item);
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer)
            {

            }
        };
    }
}