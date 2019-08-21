package compiler.graph;

import compiler.interfaces.Graph;
import compiler.interfaces.basic.IConsumer;
import compiler.interfaces.basic.Operator;

import java.util.ArrayList;

public class ParallelGraph extends Graph
{
    private int inputArity;
    private int outputArity;

    private Operator[] operators;

    public ParallelGraph(Graph... graphs)
    {
        ArrayList<Operator> o = new ArrayList<>();
        for (Graph g : graphs)
        {
            inputArity += g.getInputArity();
            outputArity += g.getOutputArity();
            o.add(g.getOperator());
        }

        operators = o.toArray(new Operator[o.size()]);
    }

    @Override
    public int getInputArity()
    {
        return inputArity;
    }

    @Override
    public int getOutputArity()
    {
        return outputArity;
    }

    @Override
    public Operator getOperator()
    {
        return new Operator("", inputArity, outputArity, 1)
        {
            @Override
            public Operator[] getParallelConstituent()
            {
                return operators;
            }

            @Override
            public void subscribe(IConsumer[] consumers)
            {
                super.subscribe(consumers);

                int readFrom = 0;
                for (int i = 0; i < operators.length; i++)
                {
                    int outputArity = operators[i].getOutputArity();
                    IConsumer[] tmp = new IConsumer[outputArity];
                    for (int j = 0; j < outputArity; j++)
                        tmp[j] = consumers[readFrom++];

                    operators[i].subscribe(tmp);
                }
            }

            @Override
            public void next(int channelIdentifier, Object item)
            {
                int operatorNo = 0;
                int channel = 0;
                int sum = 0;

                for (int i = 0; i < operators.length; i++)
                {
                    sum += operators[i].getOutputArity();
                    if (channelIdentifier < sum)
                    {
                        operatorNo = i;
                        channel = sum - channelIdentifier;
                        break;
                    }
                }

                // TODO: calculation can be made in constructor and be static
                operators[operatorNo].next(channel, item);
            }
        };
    }
}