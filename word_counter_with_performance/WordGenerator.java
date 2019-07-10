import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Date;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

public class WordGenerator extends BaseRichSpout
{
    private SpoutOutputCollector collector;
    private Random random;

    private int min = 100, max = 1000;
    private int numberOfLines;
    private int currentLine = 0;

    private int id;

    private int M, N;

    public WordGenerator(int M, int N, int id)
    {
        this.M = M;
        this.N = N;
        this.id = id;
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
    {
        this.collector = collector;
        this.random = new Random(new Date().getTime());

        this.numberOfLines = 100000;//(int) (random.nextDouble() * (max - min) + min);
    }

    public void nextTuple()
    {
        if (currentLine == 0)
        {
            Performance.WriteTimestampToFile("Start " + id);
            Performance.WriteTimestampToFile(new Date());
        }

        if (currentLine++ <= numberOfLines)
        {
            StringBuilder line = new StringBuilder();

            int minWords = 2, maxWords = 27;
            int numberOfWordsPerLine = (int) (random.nextDouble() * (maxWords - minWords) + minWords);

            for (int j = 0; j < numberOfWordsPerLine; j++)
            {
                int minLetters = 2, maxLetters = 11;
                int numberOfLettersPerWord = (int) (random.nextDouble() * (maxLetters - minLetters) + minLetters);

                for (int k = 0; k < numberOfLettersPerWord; k++)
                {
                    char c = (char) (random.nextDouble() * (122 - 97) + 97);

                    line.append(c);
                }

                line.append(" ");
            }

            collector.emit(new Values(line.toString(), false));

            if (currentLine == numberOfLines)
                collector.emit(new Values(null, true));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("payload", "endOfStream"));
    }
}