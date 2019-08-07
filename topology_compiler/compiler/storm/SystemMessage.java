package compiler.storm;

import java.io.Serializable;

public class SystemMessage implements Serializable
{
    public static class Payload
    {

    }

    public static class MeantFor extends Payload
    {
        public int tupleMeantFor;

        public MeantFor(int tupleMeantFor)
        {
            this.tupleMeantFor = tupleMeantFor;
        }
    }

    private String operatorName;
    private Payload payload;

    public SystemMessage()
    {
    }

    public SystemMessage(String operatorName, Payload payload)
    {
        this.operatorName = operatorName;
        this.payload = payload;
    }

    public String getOperatorName()
    {
        return operatorName;
    }

    public Payload getPayload()
    {
        return payload;
    }
}