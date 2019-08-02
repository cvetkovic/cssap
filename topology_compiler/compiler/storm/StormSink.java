package compiler.storm;

import org.apache.storm.topology.base.BaseBasicBolt;

import java.io.Serializable;

public class StormSink implements Serializable
{
    private static int getId = 1;
    private String stormName;
    private BaseBasicBolt bolt;

    public StormSink( BaseBasicBolt bolt)
    {
        this.stormName = "sink" + getId++;
        this.bolt = bolt;
    }

    public String getStormName()
    {
        return stormName;
    }

    public BaseBasicBolt getBolt()
    {
        return bolt;
    }
}