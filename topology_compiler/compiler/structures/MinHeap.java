package compiler.structures;

import org.apache.storm.tuple.Tuple;

import java.io.Serializable;

public class MinHeap implements Serializable
{
    private int size;
    private KV<Integer, Tuple>[] collection;

    public MinHeap()
    {
        this.size = 0;
        this.collection = new KV[1000];
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
        KV tmp = collection[i1];
        collection[i1] = collection[i2];
        collection[i2] = tmp;
    }

    public void insert(KV<Integer, Tuple> tuple)
    {
        if (size == collection.length)
        {
            // array extension
            KV<Integer, Tuple>[] newArray = new KV[collection.length * 5];
            System.arraycopy(collection, 0, newArray, 0, collection.length);
            collection = newArray;
        }

        int i = size;
        collection[i] = tuple;
        size++;

        while (i != 0 && collection[parent(i)].getK() > collection[i].getK())
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

        if (l < size && collection[l].getK() < collection[smallest].getK())
            smallest = l;

        if (r < size && collection[r].getK() < collection[smallest].getK())
            smallest = r;

        if (smallest != i)
        {
            swap(i, smallest);
            heapify(smallest);
        }
    }

    public KV<Integer, Tuple> poll()
    {
        if (size == 0)
            throw new RuntimeException("There are no elements in the heap to get the minimum.");
        else if (size == 1)
        {
            size--;
            return collection[0];
        }

        KV<Integer, Tuple> ret = collection[0];
        collection[0] = collection[size - 1];
        size--;

        heapify(0);

        return ret;
    }

    public KV<Integer, Tuple> peek()
    {
        if (size > 0)
            return collection[0];
        else
            return null;
    }
}