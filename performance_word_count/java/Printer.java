import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

public class Printer extends BaseBasicBolt
{
    private int endsReceived = 0;

    private int id;

    private int M, N;

    public Printer(int M, int N, int id)
    {
        this.M = M;
        this.N = N;
        this.id = id;
    }

    public void execute(Tuple input, BasicOutputCollector collector)
    {
        String word = input.getStringByField("word");
        Long count = input.getLongByField("count");
        boolean end = input.getBooleanByField("end");

        //System.out.println("[" + word + ", " + count + "]");

        if (end)
        {
            endsReceived++;

            if (endsReceived == N)
            {
                Performance.WriteTimestampToFile("End " + id);
                Performance.WriteTimestampToFile(new Date());

                System.out.println("=====================> END RECEIVED BY PRINTER <=====================");
            }
        }
    }


    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {

    }
}