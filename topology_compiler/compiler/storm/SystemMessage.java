package compiler.storm;

import compiler.interfaces.basic.Operator;

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
    private Operator.Operation operation;
    private Payload payload;

    public SystemMessage()
    {
    }

    public SystemMessage(String operatorName, Operator.Operation operation, Payload payload)
    {
        this.operatorName = operatorName;
        this.operation = operation;
        this.payload = payload;
    }

    public String getOperatorName()
    {
        return operatorName;
    }

    public Operator.Operation getOperation()
    {
        return operation;
    }

    public Payload getPayload()
    {
        return payload;
    }
}