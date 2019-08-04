package compiler.interfaces;

import compiler.interfaces.basic.Operator;

public abstract class ParallelComposition<A, B> extends Operator<A, B>
{
    private Operator[] consistedOf;

    public ParallelComposition(Operator[] consistedOf)
    {
        super(null,
                calculateInputArity(consistedOf),
                calculateOutputArity(consistedOf),
                1);

        this.consistedOf = consistedOf;
    }

    public Operator[] getConsistedOf()
    {
        return consistedOf;
    }

    private static int calculateInputArity(Operator[] consistedOf)
    {
        int r = 0;
        for (Operator o : consistedOf)
            r += o.getInputArity();

        return r;
    }

    private static int calculateOutputArity(Operator[] consistedOf)
    {
        int r = 0;
        for (Operator o : consistedOf)
            r += o.getOutputArity();

        return r;
    }
}