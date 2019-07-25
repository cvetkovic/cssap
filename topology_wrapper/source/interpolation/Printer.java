package interpolation;

import compiler.interfaces.Sink;
import org.apache.storm.tuple.Tuple;

public class Printer extends Sink
{
    @Override
    public void processElement(Tuple tuple)
    {
        double averageValue = tuple.getDouble(0);

        System.out.println(averageValue);
    }
}