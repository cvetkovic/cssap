package cssap;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichSpout;

public class Topology
{
    public static void main(String[] args) throws Exception
    {
        Config config = new Config();
        config.setDebug(false);

        config.put("worker.heartbeat.frequency.secs", 60);

        LocalCluster cluster = new LocalCluster();

        TopologyBuilder builder = new TopologyBuilder();

        BaseRichSpout generator = new Generator();
        BaseBasicBolt counter = new Decimator();
        BaseBasicBolt interpolation = new Interpolation();
        BaseBasicBolt printer = new Printer();

        builder.setSpout("generator", generator);
        builder.setBolt("counter", counter, 2).customGrouping("generator", new ParityCustomGrouping());
        builder.setBolt("interpolator", interpolation).shuffleGrouping("counter");
        builder.setBolt("printer", printer).shuffleGrouping("interpolator");

        cluster.submitTopology("interpolationTopology", config, builder.createTopology());

        //Thread.sleep(100000);
        //cluster.shutdown();
    }
}