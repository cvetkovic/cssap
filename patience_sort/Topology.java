package cvetkovic;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichSpout;

import java.io.File;
import java.io.PrintStream;

public class Topology
{
    public static void main(String[] args) throws Exception
    {
        // redirecting System.out to file
        /*String pathToOutput = "/home/cvetkovic/performances.txt"; // "C:\\Users\\cl160127d\\Desktop\\perf.txt"; //;
        PrintStream printStream = new PrintStream(new File(pathToOutput));
        System.setOut(printStream);*/

        Config config = new Config();

        //config.put(Config.TOPOLOGY_WORKERS, 4);
        //config.setDebug(false);
        //config.put("worker.heartbeat.frequency.secs", 60);

        LocalCluster cluster = new LocalCluster();

        TopologyBuilder builder = new TopologyBuilder();

        BaseRichSpout generator = new Generator();
        BaseBasicBolt counter = new Decimator();
        BaseBasicBolt interpolation = new Merger();
        BaseBasicBolt average = new Average();
        BaseBasicBolt printer = new Printer();

        builder.setSpout("generator", generator);
        builder.setBolt("counter", counter, 2).customGrouping("generator", new ParityCustomGrouping());
        builder.setBolt("interpolator", interpolation).shuffleGrouping("counter");
        builder.setBolt("average", average).shuffleGrouping("interpolator");
        builder.setBolt("printer", printer).shuffleGrouping("average");

        //StormSubmitter.submitTopology("InterpolationTopology", config, builder.createTopology());
        cluster.submitTopology("interpolationTopology", config, builder.createTopology());

        //Thread.sleep(100000);
        //cluster.shutdown();
    }
}