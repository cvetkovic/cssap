package compiler;

import compiler.interfaces.AtomicOperator;
import compiler.interfaces.ParallelComposition;
import compiler.interfaces.StreamComposition;
import compiler.interfaces.basic.IConsumer;
import compiler.interfaces.basic.IProducer;
import compiler.interfaces.basic.Operator;
import compiler.interfaces.lambda.Function0;
import compiler.structures.StormNode;
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
import java.util.*;

public class Graph implements Serializable
{
    private final Operator[] graphOperators;

    public Graph(Operator... consumers)
    {
        graphOperators = new Operator[consumers.length];

        for (int i = 0; i < consumers.length; i++)
            graphOperators[i] = consumers[i];
    }

    public Graph(Graph... pipelines)
    {
        int countOfNodes = 0;
        for (int i = 0; i < pipelines.length; i++)
            countOfNodes += pipelines[i].graphOperators.length;

        graphOperators = new Operator[countOfNodes];
        int i = 0;
        for (Graph p : pipelines)
            for (Operator o : p.graphOperators)
                graphOperators[i++] = o;
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

    public StormTopology getStormTopology(Function0 code, IProducer producer, IConsumer consumer)
    {
        // list of bolts made from atomic operators extracted from the topology
        int id = 0;
        List<StormNode> operators = new LinkedList<>();

        for (int i = 0; i < this.graphOperators.length; i++)
        {
            if (this.graphOperators[i] instanceof AtomicOperator)
                operators.add(new StormNode(id++, this.graphOperators[i], generateOperator((AtomicOperator) this.graphOperators[i])));
            else if (this.graphOperators[i] instanceof StreamComposition)
            {
                List<AtomicOperator> operatorList = resolveComposition(this.graphOperators[i]);
                for (AtomicOperator operator : operatorList)
                    operators.add(new StormNode(id++, operator, generateOperator(operator)));
            }
            else if (this.graphOperators[i] instanceof ParallelComposition)
            {
                List<AtomicOperator> operatorList = resolveComposition(this.graphOperators[i]);
                for (AtomicOperator operator : operatorList)
                    operators.add(new StormNode(id++, operator, generateOperator(operator)));
            }
            else
                throw new RuntimeException("Provided type of operator is not implemented.");
        }

        TopologyBuilder builder = new TopologyBuilder();

        ///////////////////////////////////////////////////////////////////////////////////////////////
        ////////////////////////////// CONNECTING THE GENERATED TOPOLOGY //////////////////////////////
        ///////////////////////////////////////////////////////////////////////////////////////////////
        for (StormNode node : operators)
        {
            int nodeId = node.getStormId();
            Operator operator = node.getOperator();
            BaseBasicBolt bolt = node.getBolt();

            BoltDeclarer declarer = builder.setBolt("operator" + nodeId, bolt, operator.getParallelismHint());
            if (operator.getOutputArity() == 1)
            {
                IConsumer consumerOfOperator = (IConsumer) operator.getMapOfConsumers().get(1);
                declarer.shuffleGrouping("operator" + getStormIdOfOperator(operators, consumerOfOperator));
            }
            else
            {
                /*MultichannelGrouping customGrouping = new MultichannelGrouping(getListOfFollowingOperators());
                for (Map.Entry<> outputConsumer : operator.getMapOfConsumers().entrySet())
                {
                    String nameOfDestination = "operator";
                    declarer.customGrouping("operator" + idOfOutput, customGrouping);
                }
                */
            }
        }
        ///////////////////////////////////////////////////////////////////////////////////////////////
        ///////////////////////////////////////////////////////////////////////////////////////////////
        ///////////////////////////////////////////////////////////////////////////////////////////////

        return builder.createTopology();
    }

    private static int getStormIdOfOperator(List<StormNode> list, IConsumer op)
    {
        for(StormNode node : list)
        {
            if (node.getOperator() == op)
                return node.getStormId();
        }

        throw new RuntimeException("No such an operator in the provided storm nodes list.");
    }

    private BaseRichSpout generateSpout(Function0 code, IProducer producer)
    {
        return new BaseRichSpout()
        {
            private SpoutOutputCollector collector;
            /*private IConsumer consumer = new IConsumer()
            {
                @Override
                public void next(int channelNumber, Object item)
                {
                    collector.emit(new Values(item));
                }
            };*/

            @Override
            public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
            {
                this.collector = collector;
                //producer.subscribe(1, consumer);
            }

            @Override
            public void nextTuple()
            {
                //producer.next(1, code.call());
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