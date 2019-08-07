package compiler;

import compiler.interfaces.Graph;
import compiler.interfaces.basic.IConsumer;
import compiler.interfaces.basic.Operator;

import java.util.HashMap;
import java.util.Map;

public class ParallelGraph extends Graph
{
    private int inputArity;
    private int outputArity;

    private Operator[] operators;
    private Map<Integer, Operator> channelMapping;

    public ParallelGraph(Operator... arrayOfOperators)
    {
        for (Operator g : arrayOfOperators)
        {
            inputArity += g.getInputArity();
            outputArity += g.getOutputArity();
        }

        this.operators = arrayOfOperators;
        channelMapping = new HashMap<>(operators.length);
        // TODO: use channelMapping to calculate order number of channel where the item will be sent
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

                operators[operatorNo].next(channel, item);
            }
        };
    }
}