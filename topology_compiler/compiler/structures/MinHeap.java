package compiler.structures;

import org.apache.storm.tuple.Tuple;
import java.io.Serializable;

public class MinHeap implements Serializable
{
    private int size;
    private int capacity;
    private Tuple[] collection;
    private String keyFieldName;

    public MinHeap(int capacity, String keyFieldName)
    {
        this.capacity = capacity;
        this.size = 0;
        this.collection = new Tuple[capacity];

        this.keyFieldName = keyFieldName;
    }

    private int parent(int i)
    {
        return (i - 1) / 2;
    }

    private int leftChild(int i)
    {
        return 2 * i + 1;
    }

    private int rightChild(int i)
    {
        return 2 * i + 2;
    }

    private void swap(int i1, int i2)
    {
        Tuple tmp = collection[i1];
        collection[i1] = collection[i2];
        collection[i2] = tmp;
    }

    public void insert(Tuple tuple)
    {
        if (size == capacity)
            throw new RuntimeException("Element cannot be added to heap because it is full.");

        int i = size;
        collection[i] = tuple;
        size++;

        while (i != 0 && collection[parent(i)].getIntegerByField(keyFieldName) > collection[i].getIntegerByField(keyFieldName))
        {
            swap(i, parent(i));
            i = parent(i);
        }
    }

    private void heapify(int i)
    {
        int l = leftChild(i);
        int r = rightChild(i);
        int smallest = i;

        if (l < size && collection[l].getIntegerByField(keyFieldName) < collection[smallest].getIntegerByField(keyFieldName))
            smallest = l;

        if (r < size && collection[r].getIntegerByField(keyFieldName) < collection[smallest].getIntegerByField(keyFieldName))
            smallest = r;

        if (smallest != i)
        {
            swap(i, smallest);
            heapify(smallest);
        }
    }

    public Tuple GetMinimum()
    {
        if (size == 0)
            throw new RuntimeException("There are no elements in the heap to get the minimum.");
        else if (size == 1)
        {
            size--;
            return collection[0];
        }

        Tuple ret = collection[0];
        collection[0] = collection[size - 1];
        size--;

        heapify(0);

        return ret;
    }

    public Tuple First()
    {
        if (size > 0)
            return collection[0];
        else
            return null;
    }
}