package compiler;

import compiler.interfaces.Graph;
import compiler.interfaces.basic.IConsumer;

import java.util.HashMap;
import java.util.Map;

public class ParallelGraph extends Graph
{
    private int inputArity;
    private int outputArity;


    public ParallelGraph(AtomicGraph[] parallelNodes, AtomicGraph[] predecessor, AtomicGraph[] successor)
    {
        for (AtomicGraph g : parallelNodes)
        {
            inputArity += g.operator.getInputArity();
            outputArity += g.operator.getOutputArity();
        }

        if (parallelNodes == null || parallelNodes.length == 0)
            throw new RuntimeException("Invalid arguments for creating graphs.");
        else if (getInputArity() != getSummedOutputArity(predecessor))
            throw new RuntimeException("Input arity of parallel composition graph does not match to output arity of operators");

        Map<AtomicGraph, Integer> sourceUsage = new HashMap<>();

        for (int k = 0; k < predecessor.length; k++)
        {
            if (sourceUsage.get(predecessor[k]) == null)
                sourceUsage.put(predecessor[k], 0);

            int old = sourceUsage.get(predecessor[k]);
            IConsumer[] tmp = new IConsumer[predecessor[k].getOutputArity() - old];
            for (int x = 0; x < tmp.length; x++)
                tmp[x] = findFirstUnusedInput(parallelNodes).getOperator();

            predecessor[k].getOperator().subscribe(tmp);

            sourceUsage.replace(predecessor[k], old + tmp.length);
        }

        int x = 0;
        for (int i = 0; i < parallelNodes.length; i++)
        {
            int writeAt = 0;
            IConsumer[] cs = new IConsumer[parallelNodes[i].getOutputArity()];
            for (; x < successor.length && writeAt < cs.length; x++)
                cs[writeAt++] = successor[x].getOperator();

            parallelNodes[i].getOperator().subscribe(cs);
        }
    }

    private Map<AtomicGraph, Integer> destinationUsage = new HashMap<>();
    private AtomicGraph findFirstUnusedInput(AtomicGraph[] list)
    {
        for (int i = 0; i < list.length; i++)
        {
            if (destinationUsage.get(list[i]) == null)
                destinationUsage.put(list[i], 0);

            int old = destinationUsage.get(list[i]);
            if (old < list[i].getInputArity())
            {
                destinationUsage.replace(list[i], old + 1);
                return list[i];
            }
            else
                continue;
        }

        return null;
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

    private int getSummedOutputArity(AtomicGraph[] graphs)
    {
        int r = 0;
        for (AtomicGraph g : graphs)
            r += g.getOutputArity();

        return r;
    }
}