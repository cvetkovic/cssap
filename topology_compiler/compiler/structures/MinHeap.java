package compiler.structures;

import compiler.storm.SystemMessage;

import java.io.Serializable;

public class MinHeap implements Serializable
{
    private int size;
    private HeapElement[] collection;

    public static class HeapElement implements Comparable<HeapElement>
    {
        private Object element;
        private SystemMessage message;

        public HeapElement(Object element, SystemMessage message)
        {
            this.element = element;
            this.message = message;
        }

        public Object getElement()
        {
            return element;
        }

        public SystemMessage getMessage()
        {
            return message;
        }

        @Override
        public int compareTo(HeapElement o)
        {
            if ((this.message.getPayloadByType(SystemMessage.MessageTypes.SEQUENCE_NUMBER) == null &&
                    this.message.getPayloadByType(SystemMessage.MessageTypes.END_OF_STREAM) == null) ||
                    (o.message.getPayloadByType(SystemMessage.MessageTypes.SEQUENCE_NUMBER) == null &&
                     o.message.getPayloadByType(SystemMessage.MessageTypes.END_OF_STREAM) == null))
                throw new RuntimeException("Item doesn't have sequence number and hence cannot be compared.");

            SystemMessage.SequenceNumber snLeft = (SystemMessage.SequenceNumber) this.message.getPayloadByType(SystemMessage.MessageTypes.SEQUENCE_NUMBER);
            if (snLeft == null)
                snLeft = (SystemMessage.SequenceNumber) this.message.getPayloadByType(SystemMessage.MessageTypes.END_OF_STREAM);

            SystemMessage.SequenceNumber snRight = (SystemMessage.SequenceNumber) o.message.getPayloadByType(SystemMessage.MessageTypes.SEQUENCE_NUMBER);
            if (snRight == null)
                snRight = (SystemMessage.SequenceNumber) o.message.getPayloadByType(SystemMessage.MessageTypes.END_OF_STREAM);

            return snLeft.compareTo(snRight);
        }
    }

    public MinHeap()
    {
        this.size = 0;
        this.collection = new HeapElement[1000];
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
        HeapElement tmp = collection[i1];
        collection[i1] = collection[i2];
        collection[i2] = tmp;
    }

    public void insert(Object tuple, SystemMessage systemMessage)
    {
        if (size == collection.length)
        {
            // array extension
            HeapElement[] newArray = new HeapElement[collection.length * 5];
            System.arraycopy(collection, 0, newArray, 0, collection.length);
            collection = newArray;
        }

        int i = size;
        collection[i] = new HeapElement(tuple, systemMessage);
        size++;

        HeapElement parent = collection[parent(i)];
        HeapElement child = collection[i];

        while (i != 0 && parent.compareTo(child) > 0)
        {
            swap(i, parent(i));
            i = parent(i);

            parent = collection[parent(i)];
            child = collection[i];
        }
    }

    private void heapify(int i)
    {
        int l = leftChild(i);
        int r = rightChild(i);
        int smallest = i;

        if (l < size && (collection[l].compareTo(collection[smallest]) < 0))
            smallest = l;

        if (l < size && (collection[r].compareTo(collection[smallest]) < 0))
            smallest = r;

        if (smallest != i)
        {
            swap(i, smallest);
            heapify(smallest);
        }
    }

    public HeapElement poll()
    {
        if (size == 0)
            throw new RuntimeException("There are no elements in the heap to get the minimum.");
        else if (size == 1)
        {
            size--;
            return collection[0];
        }

        HeapElement ret = collection[0];
        collection[0] = collection[size - 1];
        size--;

        heapify(0);

        return ret;
    }

    public HeapElement peek()
    {
        if (size > 0)
            return collection[0];
        else
            return null;
    }
}