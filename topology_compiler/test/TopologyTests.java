package compiler;

import compiler.interfaces.Graph;
import compiler.interfaces.InfiniteSource;
import compiler.interfaces.basic.IConsumer;
import compiler.interfaces.basic.Operator;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.junit.Assert;
import org.junit.Test;

import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

public class TopologyTests
{
    LocalCluster cluster = new LocalCluster();

    @Test
    public void semanticsPreservation()
    {

    }

    @Test
    public void atomicTestSourceFilterPrint()
    {
        Queue<Double> res = new ConcurrentLinkedQueue<>();

        Random r = new Random();
        InfiniteSource source = new InfiniteSource(() -> r.nextDouble());
        Operator filter = NodesFactory.filter("filter", (Double item) -> item >= 0.5);
        IConsumer printer = NodesFactory.sink("printer", (Double item) -> res.add(item));

        Graph graph = new AtomicGraph(filter);
        Operator op = graph.getOperator();
        op.subscribe(printer);
        //graph.executeLocal(source);

        cluster.submitTopology("test1", new Config(), graph.getStormTopology(source));

        boolean test = true;
        while (res.peek() != null)
        {
            for (int i = 0; i < 25; i++)
                if (res.poll() < 5)
                    test = false;

            break;
        }
        cluster.killTopology("test1");

        Assert.assertTrue(test);
    }

    @Test
    public void serialCompositionTest()
    {
        Queue<Double> res = new ConcurrentLinkedQueue<>();

        Random r = new Random();
        InfiniteSource source = new InfiniteSource(() -> r.nextDouble());

        Operator filter = NodesFactory.filter("filter", (Double item) -> item > 0.5);
        Operator map = NodesFactory.map("map", (Double item) -> 2 * item);
        Operator fold = NodesFactory.fold("fold", 0.0, (Double x, Double y) -> x + y);
        IConsumer printer = NodesFactory.sink("sink", (Double item) -> res.add(item));

        Graph graph = new SerialGraph(new AtomicGraph(filter), new AtomicGraph(map), new AtomicGraph(fold));
        Operator op = graph.getOperator();
        op.subscribe(printer);
        //graph.executeLocal(source);

        cluster.submitTopology("test2", new Config(), graph.getStormTopology(source));

        boolean test = true;
        int i = 0;
        double old, val = 0;
        while (i < 100)
        {
            old = val;
            val = res.poll();
            i++;

            if (old > val)
            {
                test = false;
                break;
            }
        }
        cluster.killTopology("test2");

        Assert.assertTrue(test);
    }
}