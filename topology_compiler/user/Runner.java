package user;

import compiler.AtomicGraph;
import compiler.NodesFactory;
import compiler.ParallelGraph;
import compiler.SerialGraph;
import compiler.interfaces.Graph;
import compiler.interfaces.InfiniteSource;
import compiler.interfaces.basic.Operator;
import compiler.interfaces.basic.Sink;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;

import java.util.Random;

public class Runner
{
    public static void main(String[] args)
    {
        orderPreservingTest2();
    }

    /*
    Test where only one of the three operators will emit an output although the input tuple was sent to all three
     */
    private static void orderPreservingTest2()
    {
        Random r = new Random();

        InfiniteSource source = new InfiniteSource(() -> r.nextDouble());
        Operator multiplier = NodesFactory.map("multiplier", (Double item) -> item * 3);
        Operator copy = NodesFactory.copy("copy", 3);
        Operator filter1 = NodesFactory.filter("filter1", (Double item) -> item < 1);
        Operator filter2 = NodesFactory.filter("filter2", (Double item) -> item >= 1 && item < 2);
        Operator filter3 = NodesFactory.filter("filter3", (Double item) -> item >= 2 && item < 3);
        Operator merge = NodesFactory.merge("merger", 3);
        Sink printer = NodesFactory.sink("printer", (item) -> System.out.println(item));

        ParallelGraph parallelGraph = new ParallelGraph(new AtomicGraph(filter1),
                new AtomicGraph(filter2),
                new AtomicGraph(filter3));

        SerialGraph serialGraph = new SerialGraph(new AtomicGraph(multiplier), new AtomicGraph(copy), parallelGraph, new AtomicGraph(merge));
        Operator op = serialGraph.getOperator();
        op.subscribe(printer);
        //serialGraph.executeLocal(source);

        new LocalCluster().submitTopology("topologyCompiler", new Config(), serialGraph.getStormTopology(source));
    }

    /*
    Test that has three lines after copy. First line never returns an output. Second returns one tuple as an output. Third returns two.
    Those operators then go to merger that has to take care about the order in which items are received. Test for end of stream signal
    and for checking whether received items by printer are in the right order.
     */
    private static void orderPreservingEndOfStreamTest()
    {
        Random r = new Random();

        InfiniteSource source = new InfiniteSource(() -> r.nextDouble());
        Operator copy = NodesFactory.copy("copy", 3);
        Operator r0 = NodesFactory.filter("f0", (Double item) -> item < -1);   // will always return false
        Operator r1 = NodesFactory.buffer("buffer");
        Operator r2 = NodesFactory.duplicate("duplicator");
        Operator merge = NodesFactory.merge("merge", 3);
        Sink printer = NodesFactory.sink("printer", (item) -> System.out.println(item));

        ParallelGraph parallelGraph = new ParallelGraph(new AtomicGraph(r0),
                new AtomicGraph(r1),
                new AtomicGraph(r2));
        SerialGraph pipeline = new SerialGraph(new AtomicGraph(copy), parallelGraph, new AtomicGraph(merge));
        pipeline.getOperator().subscribe(printer);
        //serialGraph.executeLocal(source);

        new LocalCluster().submitTopology("topologyCompiler", new Config(), pipeline.getStormTopology(source));
    }

    private static void orderPreservingMultipleBranch()
    {
        Random r = new Random();

        InfiniteSource source = new InfiniteSource(() -> r.nextDouble());
        Operator copyOuter = NodesFactory.copy("copyOuter", 3);

        Operator innerCopy = NodesFactory.copy("innerCopy", 2);
        Operator innerFilter1 = NodesFactory.filter("innerFilter1", (Double item) -> item < 0.5);
        Operator innerFilter2 = NodesFactory.filter("innerFilter2", (Double item) -> item >= 0.5);
        Operator innerMerge = NodesFactory.merge("innerMerge", 2);

        ParallelGraph innerParallel = new ParallelGraph(new AtomicGraph(innerFilter1), new AtomicGraph(innerFilter2));
        SerialGraph innerBranch = new SerialGraph(new AtomicGraph(innerCopy), innerParallel, new AtomicGraph(innerMerge));

        Operator r2 = NodesFactory.duplicate("duplicator");
        Operator merge = NodesFactory.merge("merge", 3);
        Sink printer = NodesFactory.sink("printer", (item) -> System.out.println(item));

        ParallelGraph parallelGraph = new ParallelGraph(innerBranch, new AtomicGraph(r2));
        SerialGraph pipeline = new SerialGraph(new AtomicGraph(copyOuter), parallelGraph, new AtomicGraph(merge));
        pipeline.getOperator().subscribe(printer);
        //serialGraph.executeLocal(source);

        new LocalCluster().submitTopology("topologyCompiler", new Config(), pipeline.getStormTopology(source));
    }

    /*
    Test to prove that parallel composition subscription is done right and next method works
     */
    private static void parallelCompositionTest()
    {
        Random r = new Random();
        InfiniteSource source = new InfiniteSource(() -> r.nextDouble());
        Operator copy = NodesFactory.copy("copy", 2);

        Operator filter1 = NodesFactory.filter("filter1", (Double item) -> item < 0.5);
        Operator filter2 = NodesFactory.filter("filter2", (Double item) -> item >= 0.5);

        Sink sink1 = NodesFactory.sink("sink1", (item) -> System.out.println("Printer1: " + item));
        Sink sink2 = NodesFactory.sink("sink2", (item) -> System.out.println("Printer2:                 " + item));

        Graph parallel = new ParallelGraph(new AtomicGraph(filter1), new AtomicGraph(filter2));
        Graph graph = new SerialGraph(new AtomicGraph(copy), parallel);
        Operator op = graph.getOperator();
        op.subscribe(sink1, sink2);
        //graph.executeLocal(source);

        new LocalCluster().submitTopology("topologyCompiler", new Config(), graph.getStormTopology(source));
    }

    ///////////////////////////////////////////////////////////////////
    ///////////////////// OLD TESTS
    ///////////////////////////////////////////////////////////////////

    /*private static void composeTest1()
    {
        InfiniteSource source = new InfiniteSource(() -> new Random().nextDouble());

        Operator multiplierBy10 = NodesFactory.map("mul10", 1, (Double item) -> item * 10);
        Operator multiplierBy100 = NodesFactory.map("mu100", 1, (Double item) -> item * 100);
        Operator copy = NodesFactory.robinRoundSplitter("rrSplitter", 3);
        Operator composition2 = NodesFactory.streamComposition(multiplierBy10, multiplierBy100);
        Operator compositionFinal = NodesFactory.streamComposition(composition2, copy);
        IConsumer printer1 = NodesFactory.sink((item) -> System.out.println("P1: " + item));
        IConsumer printer2 = NodesFactory.sink((item) -> System.out.println("P2: " + item));
        IConsumer printer3 = NodesFactory.sink((item) -> System.out.println("P3: " + item));

        compositionFinal.subscribe(printer1, printer2, printer3);

        Graph graph = new Graph(compositionFinal);
        graph.linkSourceToOperator("src1", source, compositionFinal);
        graph.setSinks(printer1, printer2, printer3);

        LocalCluster cluster = new LocalCluster();
        Config config = new Config();

        cluster.submitTopology("topologyCompiler", config, graph.getStormTopology());
    }*/

    /*private static void mergeTest()
    {
        Function0 generator = () -> 1.0;

        IProducer source = NodesFactory.source();
        Operator copy = NodesFactory.copy();
        Operator filterBiggerThanHalf = NodesFactory.filter(1, (Double item) -> item > 0.5);
        Operator multiplierBy1000 = NodesFactory.map(1, (Double item) -> item * 1000);
        Operator merge = NodesFactory.merge();
        IConsumer printer = NodesFactory.sink((item) -> System.out.println(item));
        IConsumer printerIndependent = NodesFactory.sink((item) -> System.out.println("Independent printer: " + item));

        source.subscribe(1, copy);
        source.subscribe(2, printerIndependent);
        copy.subscribe(1, filterBiggerThanHalf);
        copy.subscribe(2, multiplierBy1000);
        filterBiggerThanHalf.subscribe(1, merge);
        multiplierBy1000.subscribe(1, merge);
        merge.subscribe(1, printer);

        for (int i = 0; i < 4; i++)
        {
            Object data = generator.call();
            source.next(1, data);
            source.next(2, data);
        }
    }

    private static void roundRobinSplittingTest()
    {
        Function0 generator = () -> new Random().nextDouble() * 2.5;

        IProducer source = NodesFactory.source();
        Operator roundRobinSplitter = NodesFactory.robinRoundSplitter();
        IConsumer printer1 = NodesFactory.sink((item) -> System.out.println("Printer 1: " + item));
        IConsumer printer2 = NodesFactory.sink((item) -> System.out.println("Printer 2: " + item));
        IConsumer printer3 = NodesFactory.sink((item) -> System.out.println("Printer 3: " + item));

        source.subscribe(1, roundRobinSplitter);
        roundRobinSplitter.subscribe(1, printer1);
        roundRobinSplitter.subscribe(2, printer2);
        roundRobinSplitter.subscribe(3, printer3);

        for (int i = 0; i < 10; i++)
            source.next(1, generator.call());
    }

    private static void copyTest()
    {
        Function0 generator = () -> new Random().nextDouble() * 2.5;

        IProducer source = NodesFactory.source();
        Operator rootCopy = NodesFactory.copy();
        Operator copy1 = new NodesFactory().copy();
        Operator copy2 = new NodesFactory().copy();
        IConsumer printer1 = NodesFactory.sink((item) -> System.out.println("Printer 1: " + item));
        IConsumer printer2 = NodesFactory.sink((item) -> System.out.println("Printer 2: " + item));
        IConsumer printer3 = NodesFactory.sink((item) -> System.out.println("Printer 3: " + item));
        IConsumer printer4 = NodesFactory.sink((item) -> System.out.println("Printer 4: " + item));

        source.subscribe(1, rootCopy);
        rootCopy.subscribe(1, copy1);
        rootCopy.subscribe(2, copy2);

        copy1.subscribe(1, printer1);
        copy1.subscribe(2, printer2);
        copy2.subscribe(1, printer3);
        copy2.subscribe(2, printer4);

        source.next(1, generator.call());
    }

    private static void stormTest1()
    {
        Function0 generator = () -> (new KV<Integer, Double>(0, new Random().nextDouble() * 2.5));

        IProducer source = NodesFactory.source();
        Operator multiplierBy2 = NodesFactory.map((KV<Integer, Double> x) -> new KV<Integer, Double>(x.getK(), 2 * x.getV()));
        Operator multiplierBy3 = NodesFactory.map((KV<Integer, Double> x) -> new KV<Integer, Double>(x.getK(), 3 * x.getV()));
        Operator filter = NodesFactory.filter((KV<Integer, Double> x) -> x.getV() > 1);
        Operator sum = NodesFactory.fold(0.0, (x, y) -> x + ((KV<Integer, Double>) y).getV());
        IConsumer printer = NodesFactory.sink((item) -> System.out.println(item)); // here we get double as a result

        Operator composedOperator1 = NodesFactory.streamComposition(multiplierBy2, multiplierBy3);
        Operator composedOperator2 = NodesFactory.streamComposition(filter, sum);
        Graph pipeline = new Graph(new Graph(composedOperator1), new Graph(composedOperator2));

        pipeline.executeTopologyWithoutStorm(generator, source, printer);

        StormTopology topology = pipeline.getStormTopology(generator, source, printer);
        LocalCluster cluster = new LocalCluster();
        Config config = new Config();

        cluster.submitTopology("topologyCompiler", config, topology);
    }*/
}