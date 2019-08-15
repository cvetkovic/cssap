package compiler.storm;

import compiler.structures.KV;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class SystemMessage implements Serializable, Cloneable
{
    public enum MessageTypes
    {
        MEANT_FOR(0),
        INPUT_CHANNEL(1),
        SEQUENCE_NUMBER(2),
        END_OF_OUTPUT(3),
        END_OF_STREAM(4);
        // WHEN ADDING NEW MESSAGE TYPES CHANGE RETURNED VALUE IN count() METHOD

        private final int i;

        MessageTypes(int i)
        {
            this.i = i;
        }

        private static int count()
        {
            return 5;
        }
    }

    private Map<MessageTypes, Payload> payloads = new HashMap<>(MessageTypes.count());

    public abstract static class Payload implements Cloneable
    {
        @Override
        public abstract Object clone();
    }

    public static class MeantFor extends Payload
    {
        public int tupleMeantFor;
        public String operatorName;

        public MeantFor(String operatorName, int tupleMeantFor)
        {
            this.operatorName = operatorName;
            this.tupleMeantFor = tupleMeantFor;
        }

        @Override
        public Object clone()
        {
            return new MeantFor(operatorName, tupleMeantFor);
        }
    }

    public static class InputChannelSpecification extends Payload
    {
        public int inputChannel;

        public InputChannelSpecification(int inputChannel)
        {
            this.inputChannel = inputChannel;
        }

        @Override
        public Object clone()
        {
            return new InputChannelSpecification(inputChannel);
        }
    }

    public static class SequenceNumber extends Payload implements Comparable<SequenceNumber>
    {
        public int sequenceNumber;
        public SequenceNumber subsequenceNumber;

        public SequenceNumber(int sequenceNumber)
        {
            this(sequenceNumber, null);
        }

        public SequenceNumber(int sequenceNumber, SequenceNumber subsequenceNumber)
        {
            this.sequenceNumber = sequenceNumber;
            this.subsequenceNumber = subsequenceNumber;
        }

        public SequenceNumber getSequenceNumberLeaf()
        {
            if (this.subsequenceNumber == null)
                return this;
            else
                return subsequenceNumber.getSequenceNumberLeaf();
        }

        public SequenceNumber getParentOfLeaf()
        {
            SequenceNumber previous = null, current = this;
            // TODO: test this
            while (current.subsequenceNumber != null)
            {
                previous = current;
                current = current.subsequenceNumber;
            }

            return previous;
        }

        @Override
        public Object clone()
        {
            if (this.subsequenceNumber == null)
                return new SequenceNumber(sequenceNumber, null);
            else
                return new SequenceNumber(sequenceNumber, (SequenceNumber) subsequenceNumber.clone());
        }

        private static KV<Integer, SequenceNumber> getLeaf(SequenceNumber sn)
        {
            KV<Integer, SequenceNumber> kv;
            int i = 0;

            SequenceNumber prev = null, curr = sn;
            while (curr != null)
            {
                i++;
                prev = curr;
                curr = curr.subsequenceNumber;
            }

            return new KV(i, prev);
        }

        @Override
        public int compareTo(SequenceNumber o)
        {
            KV<Integer, SequenceNumber> thisSeq = getLeaf(this);
            KV<Integer, SequenceNumber> paramSeq = getLeaf(o);

            if (thisSeq.getK() < paramSeq.getK())
                return -1;
            else if (thisSeq.getK() == paramSeq.getK())
                return Integer.compare(thisSeq.getV().sequenceNumber, paramSeq.getV().sequenceNumber);
            else
                return 1;
        }
    }

    public static class EndOfOutput extends Payload
    {
        @Override
        public Object clone()
        {
            return new EndOfOutput();
        }
    }

    public static class EndOfStream extends Payload
    {
        @Override
        public Object clone()
        {
            return new EndOfStream();
        }
    }

    public void addPayload(Payload p)
    {
        if (p instanceof MeantFor)
            payloads.put(MessageTypes.MEANT_FOR, p);
        else if (p instanceof InputChannelSpecification)
            payloads.put(MessageTypes.INPUT_CHANNEL, p);
        else if (p instanceof SequenceNumber)
        {
            if (payloads.containsKey(MessageTypes.SEQUENCE_NUMBER) == false)
                payloads.put(MessageTypes.SEQUENCE_NUMBER, p);
            else
            {
                SequenceNumber root = (SequenceNumber)payloads.get(MessageTypes.SEQUENCE_NUMBER);
                root.getSequenceNumberLeaf().subsequenceNumber = (SequenceNumber)p;
            }
        }
        else if (p instanceof EndOfOutput)
            payloads.put(MessageTypes.END_OF_OUTPUT, p);
        else if (p instanceof EndOfStream)
            payloads.put(MessageTypes.END_OF_STREAM, p);
        else
            throw new RuntimeException("Not supported type of payload.");
    }

    public Payload getPayloadByType(MessageTypes type)
    {
        return payloads.get(type);
    }

    public void deletePayloadFromMessage(MessageTypes type)
    {
        // TODO: add to remove leaf subsequence
        /*if (type == MessageTypes.SEQUENCE_NUMBER)
        {
            // TODO: test this
            SequenceNumber root = (SequenceNumber) payloads.get(MessageTypes.SEQUENCE_NUMBER);
            if (root.getParentOfLeaf() == null)
                payloads.remove(MessageTypes.SEQUENCE_NUMBER);
            else
                root.getParentOfLeaf().subsequenceNumber = null;
        }
        else*/
        payloads.remove(type);
    }

    @Override
    public Object clone()
    {
        SystemMessage msg = new SystemMessage();
        for (Map.Entry<MessageTypes, Payload> entry : payloads.entrySet())
            msg.addPayload((Payload) (entry.getValue()).clone());

        return msg;
    }
}