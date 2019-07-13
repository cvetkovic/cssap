package cvetkovic;

import java.io.Serializable;

public class MinHeap implements Serializable
{
    private int size;
    private int capacity;
    private KV[] collection;

    public MinHeap(int capacity)
    {
        this.capacity = capacity;
        this.size = 0;
        this.collection = new KV[capacity];
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

    public void insert(double measurement, Integer orderNumber, long timestamp) throws Exception
    {
        if (size == capacity)
            throw new Exception("Element cannot be added to heap because it is full.");

        int i = size;
        collection[i] = new KV(measurement, orderNumber, timestamp);
        size++;

        while (i != 0 && collection[parent(i)].getOrderNumber() > collection[i].getOrderNumber())
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

        if (l < size && collection[l].getOrderNumber() < collection[smallest].getOrderNumber())
            smallest = l;

        if (r < size && collection[r].getOrderNumber() < collection[smallest].getOrderNumber())
            smallest = r;

        if (smallest != i)
        {
            swap(i, smallest);
            heapify(smallest);
        }
    }

    public KV GetMinimum() throws Exception
    {
        if (size == 0)
            throw new Exception("There are no elements in the heap to get minimum.");
        else if (size == 1)
        {
            size--;
            return collection[0];
        }

        KV ret = collection[0];
        collection[0] = collection[size - 1];
        size--;

        heapify(0);

        return ret;
    }

    public KV First()
    {
        if (size > 0)
            return collection[0];
        else
            return null;
    }
}