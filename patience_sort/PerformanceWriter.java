package cvetkovic;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.ConcurrentLinkedQueue;

public class PerformanceWriter implements Runnable, Serializable
{
    private transient ConcurrentLinkedQueue<Long> queue = new ConcurrentLinkedQueue<>();
    public PerformanceWriter() throws IOException
    {
    }

    public void addToQueue(long time)
    {
        queue.add(time);
    }

    @Override
    public void run()
    {
        while (Thread.interrupted())
        {
            /*if (bufferedWriter == null)
                continue;

            if (queue.size() == 0)
                Thread.yield();
            else
                try
                {
                    bufferedWriter.write(Long.toString(queue.poll()) + '\n');
                }
                catch (IOException ex)
                {
                    System.out.println("ERROR WRITING TO A PERFORMANCE FILE<<<<<<<<<<<<<<<");
                }*/
        }
    }

    @Override
    protected void finalize() throws Throwable
    {
        super.finalize();

        /*if (bufferedWriter != null)
        {
            bufferedWriter.close();
        }*/
    }
}