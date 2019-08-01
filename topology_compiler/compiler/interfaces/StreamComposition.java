package compiler.interfaces;

import compiler.interfaces.basic.Operator;

public abstract class StreamComposition<A,B> extends Operator<A,B>
{
    private Operator[] consistedOf;

    public StreamComposition(int inputArity, Operator[] consistedOf)
    {
        super(inputArity, 1);
        this.consistedOf = consistedOf;
    }

    public Operator[] getConsistedOf()
    {
        return consistedOf;
    }
}