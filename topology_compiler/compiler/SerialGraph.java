package compiler;

import compiler.interfaces.Graph;
import compiler.interfaces.basic.IConsumer;
import compiler.interfaces.basic.Operator;

public class SerialGraph extends Graph
{
    protected Operator[] graphs;

    public SerialGraph(Operator... graphs)
    {
        this.graphs = graphs;

        for (int i = 0; i < graphs.length - 1; i++)
            if (graphs[i].getOutputArity() != graphs[i + 1].getInputArity())
                throw new RuntimeException("Input arity of the first operator does not match the arity of the second operator.");

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

    @Override
    public Operator getOperator()
    {
        for (int i = 0; i < graphs.length - 1; i++)
        {
            // enforced in constructor that the output arity of matches the input arity
            IConsumer[] subscription = new IConsumer[graphs[i].getOutputArity()];
            for (int j = 0; j < subscription.length; j++)
                subscription[j] = graphs[i + 1];

            graphs[i].subscribe(subscription);
        }

        return new Operator("", graphs[0].getInputArity(), graphs[graphs.length - 1].getOutputArity(), 1)
        {
            @Override
            public void subscribe(IConsumer[] consumers)
            {
                super.subscribe(consumers);

                // subscribe consumer to last element in the serial graph
                graphs[graphs.length - 1].subscribe(consumers);
            }

            @Override
            public void next(int channelIdentifier, Object item)
            {
                graphs[0].next(0, item);
            }
        };
    }
}