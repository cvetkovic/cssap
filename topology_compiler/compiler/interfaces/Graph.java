package compiler.interfaces;

import compiler.AtomicGraph;
import compiler.SerialGraph;
import compiler.interfaces.basic.IConsumer;
import compiler.interfaces.basic.Operator;
import compiler.interfaces.basic.Sink;
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
    private static int uniqueGateId = 1;
    protected String name;

    public abstract int getInputArity();

    public abstract int getOutputArity();

    public abstract Operator getOperator();

    public String getName()
    {
        return name;
    }

    /**
     * Method to run the topology on a single JVM thread
     *
     * @param source A source that feeds the topology with data
     */
    public void executeLocal(Source source)
    {
        // TODO: try with null system message
        while (source.hasNext())
            getOperator().next(0, new KV(source.next(), new SystemMessage()));
    }

    /**
     * Method to compile the graph into a Storm-runnable topology
     *
     * @param source A source that feeds the topology with data
     * @return Compiled Storm topology
     */
    public StormTopology getStormTopology(Source source)
    {
        // used for faster access while compiling
        Map<IConsumer, StormBolt> cache = new HashMap<>();
        TopologyBuilder builder = new TopologyBuilder();
        // compiling source into spount
        BaseRichSpout spout = generateSpout(source);
        builder.setSpout("source", spout);
        //channelMapping.put("G" + uniqueGateId, source);
        source.getOutputGates().put(0, "E" + uniqueGateId);

        // getting reference to a first operator in the graph
        Operator operator;
        if (this instanceof AtomicGraph)
            operator = getOperator();
        else if (this instanceof SerialGraph)
            operator = ((SerialGraph) this).getConstituentGraphs()[0];
        else
            throw new RuntimeException("Parallel graph not allowed on the beginning of the topology, because that implies that the topology would have multiple sources, which is not allowed.");

        // compiling first node of a graph
        BaseBasicBolt generated = generateOperator(new AtomicGraph(operator));
        BoltDeclarer declarer = builder.setBolt(operator.getName(), generated).globalGrouping("source");
        StormBolt previous = new StormBolt(operator, generated, declarer);
        cache.put(operator, previous);
        ((Operator) previous.getOperator()).getInputGates().put("E" + uniqueGateId++, previous.inputGateCount++);

        // invocation of recursive compilation
        linkOperators(cache, builder, previous, operator);

        return builder.createTopology();
    }

    private void linkOperators(Map<IConsumer, StormBolt> cache, TopologyBuilder builder, StormBolt previous, Operator previousOperator)
    {
        for (int i = 0; i < previousOperator.getOutputArity(); i++)
        {
            if (!previousOperator.getOutputGates().containsKey(i))
                previousOperator.getOutputGates().put(i, "E" + uniqueGateId);
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
                    ///////////////////////////////////////////////////////////////
                }

                /* TODO: BUG HERE -> DO NOT CREATE CONNECTION BETWEEN NODES IF IT HAS ALREADY BEEN CREATED BY
                         OTHER TRAVERSAL
                 */

                // input channel information saving
                // TODO: check channelMapping.put first argument
                ((Operator) bolt.getOperator()).getInputGates().put("E" + uniqueGateId++, bolt.inputGateCount++);

                // continue linking
                linkOperators(cache, builder, bolt, ((Operator) bolt.getOperator()));
            }
            else if (previousOperator.getConsumers()[i] instanceof Sink)
            {
                StormBolt bolt;

                if (cache.get(previousOperator.getConsumers()[i]) == null)
                {
                    bolt = new StormBolt(previousOperator.getConsumers()[i],
                            generateSink((Sink)previousOperator.getConsumers()[i]),
                            null);

                    cache.put(previousOperator.getConsumers()[i], bolt);
                }
                else
                    bolt = cache.get(previousOperator.getConsumers()[i]);

                if (bolt.getDeclarer() == null)
                {
                    BoltDeclarer declarer = builder.setBolt(((Sink) previousOperator.getConsumers()[i]).getName(), bolt.getBolt())
                            .globalGrouping(previousOperator.getName());
                    bolt.setDeclarer(declarer);
                }
                else
                    bolt.getDeclarer().globalGrouping(previousOperator.getName());

                // input channel information saving
                if (((Sink) bolt.getOperator()).getInputGates().size() != bolt.getOperator().getInputArity())
                    ((Sink) bolt.getOperator()).getInputGates().put("E" + uniqueGateId++, bolt.inputGateCount++);
            }
        }
    }

    private int extractInputChannel(String gateID)
    {
        /*KV<Object, String> kv = channelMapping.get(gateID);

        if (kv.getK() instanceof Operator)
            return (int) ((Operator) kv.getK()).getInputGates().get(kv.getV());
        else if (kv.getK() instanceof Sink)
            return (int) ((Sink) kv.getK()).getInputGates().get(kv.getV());
        else*/
            throw new RuntimeException("Never meant to be called with provided parameter");
    }

    /**
     * Turns internal Source into a Storm spout
     *
     * @param source Internal compiler source
     * @return Storm spout
     */
    public BaseRichSpout generateSpout(Source source)
    {
        return new BaseRichSpout()
        {
            private SpoutOutputCollector collector;
            private SuccessiveNumberGenerator generator = new SuccessiveNumberGenerator();

            @Override
            public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
            {
                this.collector = collector;
            }

            @Override
            public void nextTuple()
            {
                String edgeID = source.getOutputGates().get(0).toString();

                // all the inputs go to channel zero(0)
                SystemMessage message = new SystemMessage();
                message.addPayload(new SystemMessage.InputChannelSpecification(edgeID));
                message.addPayload(new SystemMessage.SequenceNumber(generator.getCurrentState()));
                generator.next();

                collector.emit(new Values(source.next(), message));
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer)
            {
                declarer.declare(new Fields("data", "message"));
            }
        };
    }

    /**
     * Compiles internal atomic graph operator into a Storm bolt
     *
     * @param operator Atomic graph
     * @return Storm bolt
     */
    private BaseBasicBolt generateOperator(AtomicGraph operator)
    {
        return new BaseBasicBolt()
        {
            private BasicOutputCollector collector;
            /* each output channel has its own consumer so that we can make distinction
               from which channel the item was sent
             */
            private IConsumer[] internalConsumers;

            // Storm task IDs of nodes to which operator outputs
            private Integer[] taskIds;

            // order preserving structures
            private SuccessiveNumberGenerator expectingSequence;
            private MinHeap buffer;

            // common generator for whole operator
            private SuccessiveNumberGenerator subsequenceGenerator = new SuccessiveNumberGenerator();

            @Override
            public void execute(Tuple input, BasicOutputCollector collector)
            {
                this.collector = collector;
                this.subsequenceGenerator.reset();

                Object item = input.getValueByField("data");
                SystemMessage message = (SystemMessage) input.getValueByField("message");
                SystemMessage.Payload payload = message.getPayloadByType(SystemMessage.MessageTypes.INPUT_CHANNEL);

                // removing unnecessary payload
                if (message.getPayloadByType(SystemMessage.MessageTypes.MEANT_FOR) != null)
                    message.deletePayloadFromMessage(SystemMessage.MessageTypes.MEANT_FOR);

                if (item == null && operator.getInputArity() <= 1)
                    return;         // EOO won't be used

                if (message.getPayloadByType(SystemMessage.MessageTypes.END_OF_OUTPUT) != null && operator.getInputArity() <= 1)
                    message.deletePayloadFromMessage(SystemMessage.MessageTypes.END_OF_OUTPUT);

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

                    buffer.insert(input);

                    /*if (sequenceNumber.sequenceNumber == expectingSequence.getCurrentState()) &&
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
                            /*if (inputChannel != lastSentToChannel && lastSentToChannel != -1)
                                message.addPayload(new SystemMessage.EndOfOutput());*/
                    /*

                            message.deletePayloadFromMessage(SystemMessage.MessageTypes.SEQUENCE_NUMBER);

                            operator.getOperator().next(inputChannel, new KV(itemToSend.getValueByField("data"), itemToSend.getValueByField("message")));

                            lastSentToChannel = inputChannel;
                        }
                    }*/
                }
                else
                {
                    operator.getOperator().next((int)operator.getOperator().getInputGates().get(((SystemMessage.InputChannelSpecification) payload).inputChannel), new KV(item, message));

                    // here end of output must be sent
                    SystemMessage eoo = (SystemMessage)message.clone();
                    ((SystemMessage.SequenceNumber)eoo.getPayloadByType(SystemMessage.MessageTypes.SEQUENCE_NUMBER)).removeLeafSubsequence();
                    eoo.addPayload(new SystemMessage.EndOfOutput());
                    operator.getOperator().next((int)operator.getOperator().getInputGates().get(((SystemMessage.InputChannelSpecification) payload).inputChannel), new KV(null, eoo));
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
                            SystemMessage message = ((KV<Object, SystemMessage>) item).getV();
                            item = ((KV<Object, SystemMessage>) item).getK();

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
                                // sending the same sequence number to all following nodes
                                // grouping will decide what messages it will drop
                                ///////////////////////////////////////////////////////////////
                                SystemMessage.SequenceNumber sn = new SystemMessage.SequenceNumber(subsequenceGenerator.getCurrentState());
                                subsequenceGenerator.next();
                                /*
                                // do not increment sequence number if EOO is present
                                if (message.getPayloadByType(SystemMessage.MessageTypes.END_OF_OUTPUT) == null)
                                    subsequenceGenerator.next();*/
                                message.addPayload(sn);
                            }
                            else
                            {
                                ///////////////////////////////////////////////////////////////
                                //  SEQUENCE NUMBERS
                                ///////////////////////////////////////////////////////////////
                                message.addPayload(new SystemMessage.SequenceNumber(subsequenceGenerator.getCurrentState()));
                                subsequenceGenerator.next();
                            }
                            ///////////////////////////////////////////////////////////////
                            //  INPUT CHANNEL ROUTING
                            ///////////////////////////////////////////////////////////////
                            String outputEdge = operator.getOperator().getOutputGates().get(channelNumber).toString();
                            message.addPayload(new SystemMessage.InputChannelSpecification(outputEdge));

                            collector.emit(new Values(item, message));
                        }
                    };
                }

                operator.getOperator().subscribe(internalConsumers);
            }
        };
    }

    public BaseBasicBolt generateSink(Sink sink)
    {
        return new BaseBasicBolt()
        {
            @Override
            public void execute(Tuple input, BasicOutputCollector collector)
            {
                Object item = input.getValueByField("data");
                SystemMessage message = (SystemMessage) input.getValueByField("message");
                SystemMessage.Payload payload = message.getPayloadByType(SystemMessage.MessageTypes.INPUT_CHANNEL);

                sink.next((int)(sink.getInputGates().get(((SystemMessage.InputChannelSpecification) payload).inputChannel)), new KV(item, message));
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer)
            {

            }
        };
    }
}