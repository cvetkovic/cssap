package compiler.storm;

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
        public String inputChannel;

        public InputChannelSpecification(String inputChannel)
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

        @Override
        public Object clone()
        {
            if (this.subsequenceNumber == null)
                return new SequenceNumber(sequenceNumber, null);
            else
                return new SequenceNumber(sequenceNumber, (SequenceNumber) subsequenceNumber.clone());
        }

        @Override
        public int compareTo(SequenceNumber o)
        {
            SequenceNumber left = this;
            SequenceNumber right = o;

            while (left != null && right != null)
            {
                if (left.sequenceNumber < right.sequenceNumber)
                    return -1;
                else if (left.sequenceNumber == right.sequenceNumber)
                {
                    left = left.subsequenceNumber;
                    right = right.subsequenceNumber;
                }
                else
                    return 1;
            }

            if (left == null)
                return -1;
            else if (right == null)
                return 1;
            else
                return 0;
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            SequenceNumber sn = this;
            while (sn != null)
            {
                sb.append(sn.sequenceNumber);
                sb.append(".");
                sn = sn.subsequenceNumber;
            }

            return sb.toString().substring(0, sb.length() - 1); // remove last .
        }

        public void removeLeafSubsequence()
        {
            // TODO: BUG HERE
            SequenceNumber prev = null, curr = this;
            while (curr.subsequenceNumber != null)
            {
                prev = curr;
                curr = curr.subsequenceNumber;
            }

            if (prev != null)
                prev.subsequenceNumber = null;
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
                SequenceNumber root = (SequenceNumber) payloads.get(MessageTypes.SEQUENCE_NUMBER);
                root.getSequenceNumberLeaf().subsequenceNumber = (SequenceNumber) p;
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