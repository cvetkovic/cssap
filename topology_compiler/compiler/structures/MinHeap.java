package compiler.structures;

import compiler.storm.SystemMessage;
import org.apache.storm.tuple.Tuple;

import java.io.Serializable;

public class MinHeap implements Serializable
{
    private int size;
    private Tuple[] collection;

    public MinHeap()
    {
        this.size = 0;
        this.collection = new Tuple[1000];
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
        if (size == collection.length)
        {
            // array extension
            Tuple[] newArray = new Tuple[collection.length * 5];
            System.arraycopy(collection, 0, newArray, 0, collection.length);
            collection = newArray;
        }

        int i = size;
        collection[i] = tuple;
        size++;

        SystemMessage parent = (SystemMessage)collection[parent(i)].getValueByField("message");
        SystemMessage child = (SystemMessage)collection[i].getValueByField("message");

        while (i != 0 && ((SystemMessage.SequenceNumber)parent.getPayloadByType(SystemMessage.MessageTypes.SEQUENCE_NUMBER)).compareTo((SystemMessage.SequenceNumber)child.getPayloadByType(SystemMessage.MessageTypes.SEQUENCE_NUMBER)) > 0)
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

        if (l < size && ((SystemMessage.SequenceNumber)((SystemMessage)collection[l].getValueByField("message")).getPayloadByType(SystemMessage.MessageTypes.SEQUENCE_NUMBER)).compareTo(((SystemMessage.SequenceNumber)((SystemMessage)collection[smallest].getValueByField("message")).getPayloadByType(SystemMessage.MessageTypes.SEQUENCE_NUMBER))) < 0)
            smallest = l;

        if (l < size && ((SystemMessage.SequenceNumber)((SystemMessage)collection[r].getValueByField("message")).getPayloadByType(SystemMessage.MessageTypes.SEQUENCE_NUMBER)).compareTo(((SystemMessage.SequenceNumber)((SystemMessage)collection[smallest].getValueByField("message")).getPayloadByType(SystemMessage.MessageTypes.SEQUENCE_NUMBER))) < 0)
            smallest = r;

        if (smallest != i)
        {
            swap(i, smallest);
            heapify(smallest);
        }
    }

    public Tuple poll()
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

    public Tuple peek()
    {
        if (size > 0)
            return collection[0];
        else
            return null;
    }
}