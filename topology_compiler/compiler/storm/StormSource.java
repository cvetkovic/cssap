package compiler.storm;

import compiler.interfaces.basic.IConsumer;
import compiler.interfaces.InfiniteSource;
import org.apache.storm.topology.base.BaseRichSpout;

import java.io.Serializable;

public class StormSource implements Serializable
{
    private InfiniteSource source;
    private IConsumer consumer;
    private BaseRichSpout spout;
    private String name;

    public StormSource(String name, InfiniteSource source, IConsumer consumer)
    {
        this.name = name;
        this.source = source;
        this.consumer = consumer;
    }

    public InfiniteSource getSource()
    {
        return source;
    }

    public IConsumer getConsumer()
    {
        return consumer;
    }

    public BaseRichSpout getSpout()
    {
        return spout;
    }

    public void setSpout(BaseRichSpout spout)
    {
        this.spout = spout;
    }

    public String getName()
    {
        return name;
    }
}
