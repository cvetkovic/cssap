package compiler;

import compiler.interfaces.Graph;

public class SerialGraph extends Graph
{
    protected AtomicGraph[] graphs;

    public SerialGraph(AtomicGraph... graphs)
    {
        this.graphs = graphs;

        for (int i = 0; i < graphs.length - 1; i++)
        {
            // arity checking
            if (graphs[i].getOutputArity() != graphs[i + 1].getInputArity())
                throw new RuntimeException("Input arity of the first operator does not match the arity of the second operator.");

            // doing subscription
            graphs[i].operator.subscribe(graphs[i + 1].operator);
        }

        this.graphs = graphs;
    }

    @Override
    public int getInputArity()
    {
        return graphs[0].getInputArity();
    }

    @Override
    public int getOutputArity()
    {
        return graphs[graphs.length - 1].getOutputArity();
    }

    public AtomicGraph[] getGraphs()
    {
        return graphs;
    }
}