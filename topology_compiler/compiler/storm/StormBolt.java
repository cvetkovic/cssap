package compiler.storm;

import compiler.interfaces.basic.IConsumer;
import compiler.storm.groupings.MultipleOutputGrouping;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;

import java.io.Serializable;

/* Auxiliary class to help when compiling the topology to Apache Storm */
public class StormBolt implements Serializable
{
    private IConsumer operator;
    private BaseBasicBolt bolt;
    private BoltDeclarer declarer;
    private MultipleOutputGrouping customGrouping;
    public int inputGateCount = 0;

    public StormBolt(IConsumer operator, BaseBasicBolt bolt, BoltDeclarer declarer)
    {
        this.operator = operator;
        this.bolt = bolt;
        this.declarer = declarer;
    }

    public IConsumer getOperator()
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

    public MultipleOutputGrouping getCustomGrouping()
    {
        return customGrouping;
    }

    public void setCustomGrouping(MultipleOutputGrouping customGrouping)
    {
        this.customGrouping = customGrouping;
    }
}
