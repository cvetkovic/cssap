package cvetkovic;

import java.io.Serializable;
import java.util.*;

public class ImpatienceSortBuffer implements Serializable
{
    private int numberOfElements = 0;
    List<List<KV>> collection = new ArrayList<>();

    public synchronized List<KV> GetAllKVLowerThan(int emitAllTo)
    {
        // Hufmann sorting to guarantee best performances
        collection.sort(new Comparator<List<KV>>()
        {
            @Override
            public int compare(List<KV> o1, List<KV> o2)
            {
                return Integer.compare(o1.size(), o2.size());
            }
        });

        List<KV> output = new LinkedList<>();
        for (List<KV> run : collection)
        {
            for (KV kv : run)
            {
                if (kv.getOrderNumber() <= emitAllTo)
                {
                    output.add(kv);
                    numberOfElements--;
                }
            }
        }

        for (List<KV> run : collection)
            run.removeIf(p -> p.getOrderNumber() <= emitAllTo);
        collection.removeIf(p -> p.size() == 0);

        return output;
    }

    public synchronized void Insert(KV kv)
    {
        numberOfElements++;

        for (List<KV> singleRun : collection)
        {
            if (singleRun.get((singleRun.size() - 1)).getOrderNumber() <= kv.getOrderNumber())
            {
                singleRun.add(kv);
                return;
            }
        }

        List<KV> newRun = new ArrayList<>();
        newRun.add(kv);

        collection.add(newRun);
    }
}