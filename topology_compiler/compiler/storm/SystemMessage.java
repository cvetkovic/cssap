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

        public SequenceNumber getLeaf()
        {
            SequenceNumber curr = this;

            while (curr.subsequenceNumber != null)
                curr = curr.subsequenceNumber;

            return curr;
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

            if (left == null && right == null)
                return 0;
            else if (left == null)
                return -1;
            else // right == null
                return 1;
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

        public void removeLeaf()
        {
            SequenceNumber prev = null, curr = this;
            while (curr.subsequenceNumber != null)
            {
                prev = curr;
                curr = curr.subsequenceNumber;
            }

            if (prev != null)
                prev.subsequenceNumber = null;
        }

        public static boolean commonProducer(SequenceNumber s1, SequenceNumber s2)
        {
            while (s1 != null && s2 != null)
            {
                if (s1.sequenceNumber != s2.sequenceNumber && s1.subsequenceNumber != null && s2.subsequenceNumber != null)
                    return false;

                s1 = s1.subsequenceNumber;
                s2 = s2.subsequenceNumber;
            }

            if ((s1 == null && s2 != null) || (s1 != null && s2 == null))
                return false;
            else
                return true;
        }
    }

    public static class EndOfOutput extends Payload
    {
        private SequenceNumber sequenceNumber;

        public EndOfOutput(SequenceNumber sequenceNumber)
        {
            this.sequenceNumber = sequenceNumber;
        }

        @Override
        public Object clone()
        {
            return new EndOfOutput((SequenceNumber) sequenceNumber.clone());
        }
    }

    public static class EndOfStream extends SequenceNumber
    {
        public EndOfStream()
        {
            super(Integer.MAX_VALUE);
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
                root.getLeaf().subsequenceNumber = (SequenceNumber) p;
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