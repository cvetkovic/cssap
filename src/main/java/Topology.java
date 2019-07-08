import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;

import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

public class Topology
{
    private static int M = 2;
    private static int N = 2;

    private static int cnt = 0;

    public static void main(String[] args) throws Exception
    {
        Config config = new Config();
        config.setDebug(false);
        LocalCluster cluster = new LocalCluster();

        TopologyBuilder builder = new TopologyBuilder();

        BaseRichSpout generator = new WordGenerator(M, N, cnt);
        BaseRichBolt splitter = new Splitter(M, N);
        BaseBasicBolt counter = new Counter(M, N);
        BaseBasicBolt printer = new Printer(M, N, cnt++);

        builder.setSpout("generator", generator);
        builder.setBolt("splitter", splitter, M).customGrouping("generator", new SourceToSplitterRouting());
        builder.setBolt("counter", counter, N).customGrouping("splitter", new SourceToSplitterRouting());
        builder.setBolt("printer", printer).globalGrouping("counter");

        cluster.submitTopology("PerformanceMeasurerTopology" + M, config, builder.createTopology());
    }
}