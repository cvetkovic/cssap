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
        IProducer source = NodesFactory.createSource(new IActionSource<KV<Integer, Double>>()
        {
            private int number = 0;
            private Random r = new Random();

            @Override
            public KV<Integer, Double> process()
            {
                double measurement = r.nextDouble() * 2.5;

                return new KV<Integer, Double>(number++, measurement);
            }
        });
        Operator multiplier = NodesFactory.createOperator(new IActionOperator<KV<Integer, Double>>()
        {
            @Override
            public KV<Integer, Double> process(KV<Integer, Double> item)
            {
                return new KV<Integer,Double>(item.getK(), 2 * item.getV());
            }
        });
        Operator interpolator = NodesFactory.createOperator(new IActionOperator<KV<Integer, Double>>()
        {
            private double previous = 0;

            @Override
            public KV<Integer, Double> process(KV<Integer, Double> item)
            {
                double measurement = item.getV();
                double interpolatedValue = (previous + measurement) / 2;
                previous = measurement;

                return new KV<>(0,interpolatedValue);
            }
        });
        Operator average = NodesFactory.createOperator(new IActionOperator<KV<Integer, Double>>()
        {
            private long lastSentAt = 0;
            private double runningSum = 0;
            private long numberOfItems = 0;
            private int resultNumber = 0;

            private static final int INTERVAL = 1000;

            @Override
            public KV<Integer, Double> process(KV<Integer, Double> item)
            {
                runningSum += item.getV();
                numberOfItems++;

                long currentTime = new Date().getTime();
                if (currentTime - lastSentAt >= INTERVAL)
                {
                    lastSentAt = currentTime;
                    double result = runningSum / (double)numberOfItems;

                    return new KV<Integer, Double>(resultNumber++, result);
                }

                return null;
            }
        });
        IConsumer printer = NodesFactory.createSink(new IActionSink<KV<Integer,Double>>()
        {
            @Override
            public void process(KV<Integer, Double> item)
            {
                System.out.println(item.getV());
            }
        });

        Pipeline pipeline = new Pipeline(source, printer, multiplier, interpolator, average);

        StormTopology topology = pipeline.getStormTopology();
        LocalCluster cluster = new LocalCluster();
        Config config = new Config();

        cluster.submitTopology("interpolationTopology", config, topology);
    }
}