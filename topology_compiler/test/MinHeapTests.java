package compiler;

import compiler.storm.SystemMessage;
import compiler.structures.MinHeap;
import org.junit.Assert;
import org.junit.Test;

public class MinHeapTests
{
    @Test
    public void insertAndPollTest()
    {
        MinHeap heap = new MinHeap();

        SystemMessage sm7 = new SystemMessage();
        sm7.addPayload(new SystemMessage.SequenceNumber(7));
        heap.insert(7, sm7);

        SystemMessage sm8 = new SystemMessage();
        sm8.addPayload(new SystemMessage.SequenceNumber(8));
        heap.insert(8, sm8);

        SystemMessage sm5 = new SystemMessage();
        sm5.addPayload(new SystemMessage.SequenceNumber(5));
        heap.insert(5, sm5);

        SystemMessage sm6 = new SystemMessage();
        sm6.addPayload(new SystemMessage.SequenceNumber(6));
        heap.insert(6, sm6);

        SystemMessage sm1 = new SystemMessage();
        sm1.addPayload(new SystemMessage.SequenceNumber(1));
        heap.insert(1, sm1);

        SystemMessage sm4 = new SystemMessage();
        sm4.addPayload(new SystemMessage.SequenceNumber(4));
        heap.insert(4, sm4);

        SystemMessage sm2 = new SystemMessage();
        sm2.addPayload(new SystemMessage.SequenceNumber(2));
        heap.insert(2, sm2);

        SystemMessage sm3 = new SystemMessage();
        sm3.addPayload(new SystemMessage.SequenceNumber(3));
        heap.insert(3, sm3);

        Assert.assertTrue((int)heap.poll().getElement() == 1);
        Assert.assertTrue((int)heap.poll().getElement() == 2);
        Assert.assertTrue((int)heap.poll().getElement() == 3);
        Assert.assertTrue((int)heap.poll().getElement() == 4);
        Assert.assertTrue((int)heap.poll().getElement() == 5);
        Assert.assertTrue((int)heap.poll().getElement() == 6);
        Assert.assertTrue((int)heap.poll().getElement() == 7);
        Assert.assertTrue((int)heap.poll().getElement() == 8);
    }
}