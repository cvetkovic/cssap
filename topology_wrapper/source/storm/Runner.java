package storm;

import compiler.Pipeline;
import compiler.interfaces.*;
import interpolation.*;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;

public class Runner
{
    public static void main(String[] args)
    {
        LocalCluster cluster = new LocalCluster();
        Config config = new Config();

        //config.setDebug(false);

        Source source = new Generator();
        Operator multiplier = new Multiplier();
        Operator interpolator = new Interpolation("orderNumber");
        Operator average = new Average();
        Sink printer = new Printer();

        Pipeline pipeline = new Pipeline();
        pipeline.defineSource(source);
        pipeline.defineOperator(multiplier, 4, source);
        pipeline.defineOperator(interpolator, 1, multiplier);
        pipeline.defineOperator(average, 1, interpolator);
        pipeline.defineSink(printer, average);

        cluster.submitTopology("interpolationTopology", config, pipeline.getCompiledStormTopology());
    }
}