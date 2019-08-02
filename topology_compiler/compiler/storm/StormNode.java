package compiler.storm;

import compiler.interfaces.basic.Operator;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;

import java.io.Serializable;

public class StormNode implements Serializable
{
    private static int getId = 1;
    private String stormId;
    private Operator operator;
    private BaseBasicBolt bolt;
    private BoltDeclarer declarer;

    public StormNode(Operator operator, BaseBasicBolt bolt)
    {
        this.stormId = "operator" + getId++;
        this.operator = operator;
        this.bolt = bolt;
    }

    public String getStormId()
    {
        return stormId;
    }

    public Operator getOperator()
    {
        return operator;
    }

    public BaseBasicBolt getBolt()
    {
        return bolt;
    }

    public BoltDeclarer getDeclarer()
    {
        return declarer;
    }

    public void setDeclarer(BoltDeclarer declarer)
    {
        this.declarer = declarer;
    }
}