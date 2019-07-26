package user;

import compiler.NodesFactory;
import compiler.Pipeline;
import compiler.interfaces.*;
import compiler.structures.KV;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;

import java.util.Date;
import java.util.Random;

public class Runner
{
    public static void main(String[] args)
    {
        IProducer source = NodesFactory.createSource(() -> (new KV<Integer, Double>(0, new Random().nextDouble() * 2.5)));
        Operator multiplier = NodesFactory.createMap((KV<Integer, Double> x) -> new KV<Integer, Double>(x.getK(), 2 * x.getV()));
        //Operator filter = NodesFactory.createFilter((KV<Integer, Double> x) -> x.getV() > 1);
        //Operator sum = NodesFactory.createFold(0, (x, y) -> x + y);
        IConsumer printer = NodesFactory.createSink();

        Pipeline pipeline = new Pipeline(source, printer, multiplier);

        StormTopology topology = pipeline.getStormTopology();
        LocalCluster cluster = new LocalCluster();
        Config config = new Config();

        cluster.submitTopology("interpolationTopology", config, topology);
    }
}