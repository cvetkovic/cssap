package user;

import compiler.Graph;
import compiler.NodesFactory;
import compiler.interfaces.basic.IConsumer;
import compiler.interfaces.basic.InfiniteSource;
import compiler.interfaces.basic.Operator;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;

public class Runner
{
    public static void main(String[] args)
    {
        composeTest1();
    }

    private static void composeTest1()
    {
        InfiniteSource source = new InfiniteSource(() -> 1.0);

        Operator multiplierBy10 = NodesFactory.map(1, (Double item) -> item * 10);
        Operator multiplierBy100 = NodesFactory.map(1, (Double item) -> item * 100);
        Operator copy = NodesFactory.copy();
        Operator composition2 = NodesFactory.streamComposition(multiplierBy10, multiplierBy100);
        Operator compositionFinal = NodesFactory.streamComposition(composition2, copy);
        IConsumer printer1 = NodesFactory.sink((item) -> System.out.println("P1: " + item));
        IConsumer printer2 = NodesFactory.sink((item) -> System.out.println("P2: " + item));
        IConsumer printer3 = NodesFactory.sink((item) -> System.out.println("P3: " + item));

        compositionFinal.subscribe(printer1, printer2, printer3);

        Graph graph = new Graph(compositionFinal);
        graph.linkSourceToOperator(source, compositionFinal);
        graph.setSinks(printer1, printer2, printer3);

        LocalCluster cluster = new LocalCluster();
        Config config = new Config();

        cluster.submitTopology("topologyCompiler", config, graph.getStormTopology());
    }

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