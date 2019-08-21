package compiler.interfaces;

import compiler.graph.AtomicGraph;
import compiler.graph.SerialGraph;
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
        // getting reference to a first operator in the graph
        Operator operator;
        if (this instanceof AtomicGraph)
            operator = getOperator();
        else if (this instanceof SerialGraph)
            operator = ((SerialGraph) this).getConstituentGraphs()[0];
        else
            throw new RuntimeException("Parallel graph not allowed on the beginning of the topology, because that implies that the topology would have multiple sources, which is not allowed.");


        // used for faster access while compiling
        Map<IConsumer, StormBolt> cache = new HashMap<>();
        TopologyBuilder builder = new TopologyBuilder();
        // compiling source into spount
        BaseRichSpout spout = generateSpout(source);
        builder.setSpout("source", spout);
        //channelMapping.put("G" + uniqueGateId, source);
        source.getOutputGates().put(0, new KV("E" + uniqueGateId, getOperator().getName()));

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
            IConsumer[] successors = previousOperator.getConsumers();
            ///////////////////////////////////////////////////////////////
            //  PARALLEL COMPOSITION CONSTITUENTS EXTRACTION
            ///////////////////////////////////////////////////////////////
            if (successors[i] instanceof Operator && ((Operator) successors[i]).getParallelConstituent() != null)
                successors = ((Operator) successors[i]).getParallelConstituent();
            ///////////////////////////////////////////////////////////////

            if (!previousOperator.getOutputGates().containsKey(i))
                previousOperator.getOutputGates().put(i, new KV("E" + uniqueGateId, successors[i]));

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
                            generateSink((Sink) previousOperator.getConsumers()[i]),
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
            private boolean endOfStreamSent = false;

            // all the inputs go to channel zero(0)
            private String edgeID;

            @Override
            public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
            {
                this.collector = collector;
                this.edgeID = ((KV) source.getOutputGates().get(0)).getK().toString();
            }

            @Override
            public void nextTuple()
            {
                if (!source.hasNext())
                {
                    // send end of stream only once
                    if (!endOfStreamSent)
                    {
                        SystemMessage message = new SystemMessage();
                        message.addPayload(new SystemMessage.InputChannelSpecification(edgeID));
                        message.addPayload(new SystemMessage.EndOfStream());
                        endOfStreamSent = true;

                        collector.emit(new Values(null, message));
                    }

                    return;
                }
                else    // SEND PRODUCED ITEMS
                {
                    SystemMessage message = new SystemMessage();
                    message.addPayload(new SystemMessage.InputChannelSpecification(edgeID));
                    message.addPayload(new SystemMessage.SequenceNumber(generator.getCurrentState()));
                    generator.next();

                    collector.emit(new Values(source.next(), message));
                }
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
               from which channel the item was sent */
            private IConsumer[] internalConsumers;

            // Storm task IDs of nodes to which operator outputs
            private Map<String, Integer> taskIds = new HashMap<>();

            // order preserving structures
            private MinHeap[] inputChannelBuffer;
            private boolean[] inputChannelReceived;
            private int inputChannelReceivedCounter;
            private MinHeap.HeapElement lastProcessed;

            // counter for end of stream markers
            private int endOfStreamReceived;

            // common generator for whole operator
            private SuccessiveNumberGenerator subsequenceGenerator = new SuccessiveNumberGenerator();

            private void flushBuffer(int inputChannel)
            {
                do
                {
                    if (inputChannelReceivedCounter != operator.getOperator().getInputArity())
                        return;

                    int minIndex = 0;
                    boolean allEnd = true;
                    for (int i = 0; i < inputChannelBuffer.length; i++)
                    {
                        if (inputChannelBuffer[minIndex].peek().compareTo(inputChannelBuffer[i].peek()) > 0)
                            minIndex = i;

                        if (inputChannelBuffer[i].peek().getMessage().getPayloadByType(SystemMessage.MessageTypes.END_OF_STREAM) == null)
                            allEnd = false;
                    }

                    if (allEnd)
                    {
                        endOfStreamReceived = operator.getInputArity();

                        SystemMessage msg = new SystemMessage();
                        msg.addPayload(new SystemMessage.EndOfStream());

                        for (int i = 0; i < internalConsumers.length; i++)
                            internalConsumers[i].next(i, new KV(null, msg.clone()));

                        return;
                    }

                    MinHeap.HeapElement elem = inputChannelBuffer[minIndex].poll();
                    Object item = elem.getElement();
                    SystemMessage message = elem.getMessage();

                    if (item != null)
                        operator.getOperator().next(inputChannel, new KV(item, message));

                    if (inputChannelBuffer[minIndex].peek() == null)
                    {
                        inputChannelReceived[minIndex] = false;
                        inputChannelReceivedCounter--;

                        break;
                    }
                } while (true);
            }

            @Override
            public void execute(Tuple input, BasicOutputCollector collector)
            {
                if (endOfStreamReceived >= operator.getOperator().getInputArity())  // THE TOPOLOGY HAS FINISHED COMPUTING, NO WORK TO BE DONE MORE
                    return;

                this.collector = collector;
                this.subsequenceGenerator.reset();

                Object item = input.getValueByField("data");
                SystemMessage message = (SystemMessage) input.getValueByField("message");
                int inputChannel = (int) operator.getOperator().getInputGates().get(((SystemMessage.InputChannelSpecification) message.getPayloadByType(SystemMessage.MessageTypes.INPUT_CHANNEL)).inputChannel);

                // removing payload left from grouping
                if (message.getPayloadByType(SystemMessage.MessageTypes.MEANT_FOR) != null)
                    message.deletePayloadFromMessage(SystemMessage.MessageTypes.MEANT_FOR);

                ////////////////////////////////////////////////////////////////////////////////////////////////////////
                //  ORDER SHOULD BE PRESERVED IF TRUE
                ////////////////////////////////////////////////////////////////////////////////////////////////////////
                if (operator.getInputArity() > 1)
                {
                    if (inputChannelReceived[inputChannel] == false)
                    {
                        inputChannelReceived[inputChannel] = true;
                        inputChannelReceivedCounter++;
                    }

                    if (message.getPayloadByType(SystemMessage.MessageTypes.END_OF_STREAM) != null)
                    {
                        SystemMessage msg = new SystemMessage();
                        msg.addPayload(new SystemMessage.EndOfStream());

                        inputChannelBuffer[inputChannel].insert(null, msg);
                    }
                    else
                        inputChannelBuffer[inputChannel].insert(item, message);

                    flushBuffer(inputChannel);
                }
                else // inputArity == 1
                {
                    ////////////////////////////////////////////////////////////////////////////////////////////////////////
                    //  END OF OUTPUT -> separate algorithm for different input arity
                    ////////////////////////////////////////////////////////////////////////////////////////////////////////
                    if (message.getPayloadByType(SystemMessage.MessageTypes.END_OF_STREAM) != null)
                    {
                        endOfStreamReceived++;

                        SystemMessage msg = new SystemMessage();
                        msg.addPayload(new SystemMessage.EndOfStream());

                        for (int i = 0; i < internalConsumers.length; i++)
                            internalConsumers[i].next(i, new KV(null, msg.clone()));

                        return;
                    }
                    ////////////////////////////////////////////////////////////////////////////////////////////////////////

                    ////////////////////////////////////////////////////////////////////////////////////////////////////////
                    //  SENDING DATA TO THE OPERATOR
                    ////////////////////////////////////////////////////////////////////////////////////////////////////////
                    if (item != null)
                        operator.getOperator().next(inputChannel, new KV(item, message));

                    ////////////////////////////////////////////////////////////////////////////////////////////////////////
                    //  END OF OUTPUT IN CASE IT IS IMPLEMENTED SHOULD GO BELOW
                    ////////////////////////////////////////////////////////////////////////////////////////////////////////
                    //  The code below requires that operator has ability to process null items. It was done this
                    //  way to be able to simplify the code because otherwise source must be distinguished from operators
                    //  which requires addition coding
                    ////////////////////////////////////////////////////////////////////////////////////////////////////////
                    for (int i = 0; i < internalConsumers.length; i++)
                    {
                        if (item == null)   // just pass the item
                            internalConsumers[i].next(i, new KV(item, message.clone()));
                        else
                        {
                            SystemMessage cl = (SystemMessage) message.clone();
                            cl.addPayload(new SystemMessage.SequenceNumber(Integer.MAX_VALUE));

                            internalConsumers[i].next(i, new KV(null, cl));
                        }
                    }
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

                while (nextOperatorNames.hasNext())
                {
                    String opName = nextOperatorNames.next();
                    taskIds.put(opName, context.getComponentTasks(opName).get(0));
                }

                internalConsumers = new IConsumer[operator.getOutputArity()];

                inputChannelReceived = new boolean[operator.getInputArity()];
                inputChannelBuffer = new MinHeap[operator.getInputArity()];
                for (int i = 0; i < inputChannelBuffer.length; i++)
                    inputChannelBuffer[i] = new MinHeap();

                for (int i = 0; i < internalConsumers.length; i++)
                {
                    internalConsumers[i] = new IConsumer()
                    {
                        @Override
                        public String getName()
                        {
                            throw new RuntimeException("This method was never supposed to be called.");
                        }

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
                                String nextOperatorName = ((IConsumer) (((KV) operator.getOperator().getOutputGates().get(channelNumber)).getV())).getName();
                                //int taskGoingTo = taskIds[channelNumber];
                                message.addPayload(new SystemMessage.MeantFor(operator.getOperator().getName(), taskIds.get(nextOperatorName)));
                            }

                            ///////////////////////////////////////////////////////////////
                            //  SEQUENCE NUMBERS
                            ///////////////////////////////////////////////////////////////
                            // sending the same sequence number to all following nodes
                            // grouping will decide what messages it will drop
                            ///////////////////////////////////////////////////////////////
                            if (item != null)
                            {
                                message.addPayload(new SystemMessage.SequenceNumber(subsequenceGenerator.getCurrentState()));
                                subsequenceGenerator.next();
                            }

                            ///////////////////////////////////////////////////////////////
                            //  OUTPUT CHANNEL ROUTING
                            ///////////////////////////////////////////////////////////////
                            String outputEdge = ((KV) operator.getOperator().getOutputGates().get(channelNumber)).getK().toString();
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
            private boolean halted = false;

            @Override
            public void execute(Tuple input, BasicOutputCollector collector)
            {
                if (halted)
                    return;

                Object item = input.getValueByField("data");
                SystemMessage message = (SystemMessage) input.getValueByField("message");
                SystemMessage.Payload payload = message.getPayloadByType(SystemMessage.MessageTypes.INPUT_CHANNEL);
                int inputChannel = (int) (sink.getInputGates().get(((SystemMessage.InputChannelSpecification) payload).inputChannel));

                if (message.getPayloadByType(SystemMessage.MessageTypes.END_OF_STREAM) != null)
                {
                    halted = true;
                    sink.next(inputChannel, new KV(null, message));
                }

                if (item != null)
                    sink.next(inputChannel, new KV(item, message));
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer)
            {

            }
        };
    }
}