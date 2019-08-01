package compiler.structures;

import compiler.interfaces.basic.Operator;
import org.apache.storm.topology.base.BaseBasicBolt;

public class StormNode
{
    private int stormId;
    private Operator operator;
    private BaseBasicBolt bolt;

    public StormNode(int stormId, Operator operator, BaseBasicBolt bolt)
    {
        this.stormId = stormId;
        this.operator = operator;
        this.bolt = bolt;
    }

    public int getStormId()
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
}