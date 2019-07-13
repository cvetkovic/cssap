package cvetkovic;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Date;
import java.util.Map;

public class Interpolation extends BaseBasicBolt
{
    private int expecting = 0;
    //private MinHeap minHeap;
    //private PerformanceWriter performanceWriter;
    //private Thread performanceWriterThread;
    private transient BufferedWriter bufferedWriter;

    public Interpolation()
    {
        //minHeap = new MinHeap(10000);
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context)
    {
        super.prepare(stormConf, context);

        try
        {
            bufferedWriter = new BufferedWriter(new FileWriter("/home/cvetkovic/performance.txt", true));

            /*this.performanceWriter = new PerformanceWriter();
            this.performanceWriterThread = new Thread(performanceWriter);

            performanceWriterThread.start();*/
        }
        catch (Exception ex)
        {

        }
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector)
    {
        double measurement = input.getDouble(0);
        //int number = input.getInteger(1);
        long timestamp = input.getLong(2);

        try
        {
            /*minHeap.insert(measurement, number, timestamp);

            if (number == expecting)
                flushBuffer(collector);*/
            /*else
                System.out.println("Waiting for element in row to come.");*/
            long msToProcess = (new Date()).getTime() - timestamp;

            double interpolatedValue = (previous + measurement) / 2;
            collector.emit(new Values(interpolatedValue));
            collector.emit(new Values(measurement));
            previous = measurement;

            try
            {
                bufferedWriter.write(Long.toString(msToProcess) + '\n');
                bufferedWriter.flush();
            }
            catch (IOException ex)
            {
                System.out.println("ERROR WRITING TO A PERFORMANCE FILE<<<<<<<<<<<<<<<");
            }
        } catch (Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("data"));
    }

    // private int cnt = 0;
    // private final static int WINDOW_SIZE = 10;
    // private double[] buffer = new double[WINDOW_SIZE];
    private double previous = 0;

    /*private void flushBuffer(BasicOutputCollector collector) throws Exception
    {
        while (minHeap.First() != null && minHeap.First().getOrderNumber() == expecting)
        {
            KV kv = minHeap.GetMinimum();



            expecting++;
        }
    }*/

    @Override
    protected void finalize() throws Throwable
    {
        super.finalize();

        bufferedWriter.close();
    }
}