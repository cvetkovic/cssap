package compiler;

import compiler.storm.SystemMessage;
import org.junit.*;

public class SystemMessageTests
{
    // sequence number nesting and toString
    @Test
    public void subsequenceTest1()
    {
        SystemMessage msg = new SystemMessage();

        msg.addPayload(new SystemMessage.SequenceNumber(1));
        Assert.assertTrue(msg.getPayloadByType(SystemMessage.MessageTypes.SEQUENCE_NUMBER).toString().equals("1"));

        msg.addPayload(new SystemMessage.SequenceNumber(2));
        Assert.assertTrue(msg.getPayloadByType(SystemMessage.MessageTypes.SEQUENCE_NUMBER).toString().equals("1.2"));

        msg.addPayload(new SystemMessage.SequenceNumber(3));
        Assert.assertTrue(msg.getPayloadByType(SystemMessage.MessageTypes.SEQUENCE_NUMBER).toString().equals("1.2.3"));

        msg.addPayload(new SystemMessage.SequenceNumber(4));
        Assert.assertTrue(msg.getPayloadByType(SystemMessage.MessageTypes.SEQUENCE_NUMBER).toString().equals("1.2.3.4"));
    }

    // getLeafNode
    @Test
    public void subsequenceTest2()
    {
        SystemMessage msg = new SystemMessage();

        msg.addPayload(new SystemMessage.SequenceNumber(1));
        msg.addPayload(new SystemMessage.SequenceNumber(2));
        msg.addPayload(new SystemMessage.SequenceNumber(3));
        msg.addPayload(new SystemMessage.SequenceNumber(4));

        SystemMessage.SequenceNumber sn = (SystemMessage.SequenceNumber)msg.getPayloadByType(SystemMessage.MessageTypes.SEQUENCE_NUMBER);
        Assert.assertTrue(sn.getLeaf().toString().equals("4"));
    }

    // cloning test
    @Test
    public void subsequenceTest3()
    {
        SystemMessage msg = new SystemMessage();

        msg.addPayload(new SystemMessage.SequenceNumber(1));
        msg.addPayload(new SystemMessage.SequenceNumber(2));
        msg.addPayload(new SystemMessage.SequenceNumber(3));
        msg.addPayload(new SystemMessage.SequenceNumber(4));

        SystemMessage.SequenceNumber sn = (SystemMessage.SequenceNumber)msg.getPayloadByType(SystemMessage.MessageTypes.SEQUENCE_NUMBER);
        Assert.assertTrue(sn.clone().toString().equals("1.2.3.4"));
    }

    // compareTo
    @Test
    public void subsequenceTest4()
    {
        SystemMessage msg1 = new SystemMessage();
        msg1.addPayload(new SystemMessage.SequenceNumber(1));
        msg1.addPayload(new SystemMessage.SequenceNumber(2));
        SystemMessage.SequenceNumber sn1 = (SystemMessage.SequenceNumber)msg1.getPayloadByType(SystemMessage.MessageTypes.SEQUENCE_NUMBER);

        SystemMessage msg2 = new SystemMessage();
        msg2.addPayload(new SystemMessage.SequenceNumber(1));
        msg2.addPayload(new SystemMessage.SequenceNumber(2));
        SystemMessage.SequenceNumber sn2 = (SystemMessage.SequenceNumber)msg2.getPayloadByType(SystemMessage.MessageTypes.SEQUENCE_NUMBER);
        Assert.assertTrue(sn1.compareTo(sn2) == 0);

        SystemMessage msg3 = new SystemMessage();
        msg3.addPayload(new SystemMessage.SequenceNumber(1));
        msg3.addPayload(new SystemMessage.SequenceNumber(3));
        SystemMessage.SequenceNumber sn3 = (SystemMessage.SequenceNumber)msg3.getPayloadByType(SystemMessage.MessageTypes.SEQUENCE_NUMBER);
        Assert.assertTrue(sn1.compareTo(sn3) < 0);

        SystemMessage msg4 = new SystemMessage();
        msg4.addPayload(new SystemMessage.SequenceNumber(1));
        msg4.addPayload(new SystemMessage.SequenceNumber(1));
        SystemMessage.SequenceNumber sn4 = (SystemMessage.SequenceNumber)msg4.getPayloadByType(SystemMessage.MessageTypes.SEQUENCE_NUMBER);
        Assert.assertTrue(sn1.compareTo(sn4) > 0);

        SystemMessage msg5 = new SystemMessage();
        msg5.addPayload(new SystemMessage.SequenceNumber(1));
        msg5.addPayload(new SystemMessage.SequenceNumber(3));
        msg5.addPayload(new SystemMessage.SequenceNumber(3));
        SystemMessage.SequenceNumber sn5 = (SystemMessage.SequenceNumber)msg5.getPayloadByType(SystemMessage.MessageTypes.SEQUENCE_NUMBER);
        Assert.assertTrue(sn1.compareTo(sn5) < 0);

        SystemMessage msg6 = new SystemMessage();
        msg6.addPayload(new SystemMessage.SequenceNumber(1));
        msg6.addPayload(new SystemMessage.SequenceNumber(5));
        msg6.addPayload(new SystemMessage.SequenceNumber(6));
        SystemMessage.SequenceNumber sn6 = (SystemMessage.SequenceNumber)msg6.getPayloadByType(SystemMessage.MessageTypes.SEQUENCE_NUMBER);
        Assert.assertTrue(sn1.compareTo(sn6) < 0);
    }

    // remove leaf
    @Test
    public void subsequenceTest5()
    {
        SystemMessage msg = new SystemMessage();

        msg.addPayload(new SystemMessage.SequenceNumber(1));
        msg.addPayload(new SystemMessage.SequenceNumber(2));
        msg.addPayload(new SystemMessage.SequenceNumber(3));
        msg.addPayload(new SystemMessage.SequenceNumber(4));

        SystemMessage.SequenceNumber sn = (SystemMessage.SequenceNumber)msg.getPayloadByType(SystemMessage.MessageTypes.SEQUENCE_NUMBER);
        Assert.assertTrue(sn.toString().equals("1.2.3.4"));

        sn.removeLeaf();
        Assert.assertTrue(sn.toString().equals("1.2.3"));

        sn.removeLeaf();
        Assert.assertTrue(sn.toString().equals("1.2"));

        sn.removeLeaf();
        Assert.assertTrue(sn.toString().equals("1"));

        sn.removeLeaf();
        Assert.assertTrue(sn.toString().equals("1"));
    }

    // common producer test
    @Test
    public void commonProducerSequenceTest()
    {
        SystemMessage msg1 = new SystemMessage();
        msg1.addPayload(new SystemMessage.SequenceNumber(1));
        msg1.addPayload(new SystemMessage.SequenceNumber(2));
        msg1.addPayload(new SystemMessage.SequenceNumber(3));
        SystemMessage.SequenceNumber sn1 = (SystemMessage.SequenceNumber)msg1.getPayloadByType(SystemMessage.MessageTypes.SEQUENCE_NUMBER);

        SystemMessage msg2 = new SystemMessage();
        msg2.addPayload(new SystemMessage.SequenceNumber(1));
        msg2.addPayload(new SystemMessage.SequenceNumber(2));
        msg2.addPayload(new SystemMessage.SequenceNumber(5));
        SystemMessage.SequenceNumber sn2 = (SystemMessage.SequenceNumber)msg2.getPayloadByType(SystemMessage.MessageTypes.SEQUENCE_NUMBER);

        SystemMessage msg3 = new SystemMessage();
        msg3.addPayload(new SystemMessage.SequenceNumber(1));
        msg3.addPayload(new SystemMessage.SequenceNumber(2));
        SystemMessage.SequenceNumber sn3 = (SystemMessage.SequenceNumber)msg3.getPayloadByType(SystemMessage.MessageTypes.SEQUENCE_NUMBER);

        SystemMessage msg4 = new SystemMessage();
        msg4.addPayload(new SystemMessage.SequenceNumber(2));
        msg4.addPayload(new SystemMessage.SequenceNumber(3));
        msg4.addPayload(new SystemMessage.SequenceNumber(4));
        msg4.addPayload(new SystemMessage.SequenceNumber(1));
        SystemMessage.SequenceNumber sn4 = (SystemMessage.SequenceNumber)msg4.getPayloadByType(SystemMessage.MessageTypes.SEQUENCE_NUMBER);

        Assert.assertTrue(SystemMessage.SequenceNumber.commonProducer(sn1, sn2) == true);
        Assert.assertTrue(SystemMessage.SequenceNumber.commonProducer(sn2, sn3) == false);
        Assert.assertTrue(SystemMessage.SequenceNumber.commonProducer(sn3, sn4) == false);
        Assert.assertTrue(SystemMessage.SequenceNumber.commonProducer(sn1, sn4) == false);
    }

    // whole system message cloning test
    @Test
    public void systemMessageCloning()
    {
        // TODO: write this test
    }
}