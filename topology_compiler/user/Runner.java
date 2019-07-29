package user;

import compiler.NodesFactory;
import compiler.Pipeline;
import compiler.interfaces.*;
import compiler.structures.KV;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;

import java.util.Random;

public class Runner
{
    public static void main(String[] args)
    {
        IProducer source = NodesFactory.createSource(() -> (new KV<Integer, Double>(0, new Random().nextDouble() * 2.5)));
        Operator multiplierBy2 = NodesFactory.createMap((KV<Integer, Double> x) -> new KV<Integer, Double>(x.getK(), 2 * x.getV()));
        Operator multiplierBy3 = NodesFactory.createMap((KV<Integer, Double> x) -> new KV<Integer, Double>(x.getK(), 3 * x.getV()));
        Operator filter = NodesFactory.createFilter((KV<Integer, Double> x) -> x.getV() > 1);
        Operator sum = NodesFactory.createFold(0.0, (x, y) -> x + ((KV<Integer, Double>) y).getV());
        IConsumer printer = NodesFactory.createSink(); // here we get double as a result

        Operator composedOperator1 = NodesFactory.composeOperator(multiplierBy2, multiplierBy3);
        Operator composedOperator2 = NodesFactory.composeOperator(filter, sum);
        Pipeline pipeline = new Pipeline(new Pipeline(composedOperator1), new Pipeline(composedOperator2));
        //Pipeline pipeline = new Pipeline(multiplierBy2, multiplierBy3, filter, sum);

        //pipeline.executeTopologyWithoutStorm(source, printer);

        StormTopology topology = pipeline.getStormTopology(source, printer);
        LocalCluster cluster = new LocalCluster();
        Config config = new Config();

        cluster.submitTopology("topologyCompiler", config, topology);
    }
}