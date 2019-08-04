package compiler.interfaces;

import compiler.interfaces.basic.Operator;

public abstract class AtomicOperator<A,B> extends Operator<A,B>
{
    // here parameter parallelismHint is used for internal purposes and NodesFactory set it
    // when creating anonymous classes
    public AtomicOperator(String name, int inputArity, int outputArity, int parallelismHint)
    {
        super(name, inputArity, outputArity, parallelismHint);
    }

    public AtomicOperator(String name, int inputArity, int outputArity, int parallelismHint, Operation operation)
    {
        super(name, inputArity, outputArity, parallelismHint, operation);
    }
}