package user;

import compiler.NodesFactory;
import compiler.Pipeline;
import compiler.interfaces.IConsumer;
import compiler.interfaces.IProducer;
import compiler.interfaces.Operator;
import compiler.interfaces.lambda.Function0;
import compiler.structures.KV;

import java.util.Random;

public class Runner
{
    public static void main(String[] args)
    {
        composeTest1();
    }

    private static void composeTest1()
    {
        Function0 generator = () -> 1.0;

        IProducer source = NodesFactory.createSource();
        Operator multiplierBy10 = NodesFactory.createMap(1, (Double item) -> item * 10);
        Operator multiplierBy100 = NodesFactory.createMap(1, (Double item) -> item * 100);
        Operator copy = NodesFactory.createCopy();
        Operator composition = NodesFactory.createStreamComposition(1, multiplierBy10, multiplierBy100, copy);
        IConsumer printer1 = NodesFactory.createSink((item) -> System.out.println("P1: " + item));
        IConsumer printer2 = NodesFactory.createSink((item) -> System.out.println("P2: " + item));
        IConsumer printer3 = NodesFactory.createSink((item) -> System.out.println("P3: " + item));

        source.subscribe(1, composition);
        composition.subscribe(1, printer1);
        composition.subscribe(2, printer2);
        composition.subscribe(3, printer3);

        for (int i = 0; i < 4; i++)
        {
            Object data = generator.call();
            source.next(1, data);
        }
    }

    private static void mergeTest()
    {
        Function0 generator = () -> 1.0;

        IProducer source = NodesFactory.createSource();
        Operator copy = NodesFactory.createCopy();
        Operator filterBiggerThanHalf = NodesFactory.createFilter(1, (Double item) -> item > 0.5);
        Operator multiplierBy1000 = NodesFactory.createMap(1, (Double item) -> item * 1000);
        Operator merge = NodesFactory.createMerge();
        IConsumer printer = NodesFactory.createSink((item) -> System.out.println(item));
        IConsumer printerIndependent = NodesFactory.createSink((item) -> System.out.println("Independent printer: " + item));

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

        IProducer source = NodesFactory.createSource();
        Operator roundRobinSplitter = NodesFactory.createSplitterRoundRobin();
        IConsumer printer1 = NodesFactory.createSink((item) -> System.out.println("Printer 1: " + item));
        IConsumer printer2 = NodesFactory.createSink((item) -> System.out.println("Printer 2: " + item));
        IConsumer printer3 = NodesFactory.createSink((item) -> System.out.println("Printer 3: " + item));

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

        IProducer source = NodesFactory.createSource();
        Operator rootCopy = NodesFactory.createCopy();
        Operator copy1 = new NodesFactory().createCopy();
        Operator copy2 = new NodesFactory().createCopy();
        IConsumer printer1 = NodesFactory.createSink((item) -> System.out.println("Printer 1: " + item));
        IConsumer printer2 = NodesFactory.createSink((item) -> System.out.println("Printer 2: " + item));
        IConsumer printer3 = NodesFactory.createSink((item) -> System.out.println("Printer 3: " + item));
        IConsumer printer4 = NodesFactory.createSink((item) -> System.out.println("Printer 4: " + item));

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

        IProducer source = NodesFactory.createSource();
        Operator multiplierBy2 = NodesFactory.createMap((KV<Integer, Double> x) -> new KV<Integer, Double>(x.getK(), 2 * x.getV()));
        Operator multiplierBy3 = NodesFactory.createMap((KV<Integer, Double> x) -> new KV<Integer, Double>(x.getK(), 3 * x.getV()));
        Operator filter = NodesFactory.createFilter((KV<Integer, Double> x) -> x.getV() > 1);
        Operator sum = NodesFactory.createFold(0.0, (x, y) -> x + ((KV<Integer, Double>) y).getV());
        IConsumer printer = NodesFactory.createSink((item) -> System.out.println(item)); // here we get double as a result

        Operator composedOperator1 = NodesFactory.createStreamComposition(multiplierBy2, multiplierBy3);
        Operator composedOperator2 = NodesFactory.createStreamComposition(filter, sum);
        Pipeline pipeline = new Pipeline(new Pipeline(composedOperator1), new Pipeline(composedOperator2));

        pipeline.executeTopologyWithoutStorm(generator, source, printer);

        /*StormTopology topology = pipeline.getStormTopology(generator, source, printer);
        LocalCluster cluster = new LocalCluster();
        Config config = new Config();

        cluster.submitTopology("topologyCompiler", config, topology);*/
    }
}