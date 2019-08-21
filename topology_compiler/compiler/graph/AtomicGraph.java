package compiler.graph;

import compiler.interfaces.Graph;
import compiler.interfaces.basic.Operator;

public class AtomicGraph extends Graph
{
    protected Operator operator;

    public AtomicGraph(Operator operator)
    {
        this.operator = operator;
    }

    public Operator getOperator()
    {
        return operator;
    }

    @Override
    public int getInputArity()
    {
        return operator.getInputArity();
    }

    @Override
    public int getOutputArity()
    {
        return operator.getOutputArity();
    }
}