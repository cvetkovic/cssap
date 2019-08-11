package compiler.storm;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class SystemMessage implements Serializable
{
    public enum MessageTypes
    {
        MEANT_FOR(0),
        INPUT_CHANNEL(1),
        SEQUENCE_NUMBER(2),
        END_OF_OUTPUT(3);
        // WHEN ADDING NEW MESSAGE TYPES CHANGE RETURNED VALUE IN count() METHOD

        private final int i;

        MessageTypes(int i)
        {
            this.i = i;
        }

        private static int count()
        {
            return 4;
        }
    }

    private Map<MessageTypes, Payload> payloads = new HashMap<>(MessageTypes.count());

    public static class Payload
    {
    }

    public static class MeantFor extends Payload
    {
        public int tupleMeantFor;
        public String operatorName;

        public MeantFor(String operatorName, int tupleMeantFor)
        {
            this.tupleMeantFor = tupleMeantFor;
            this.operatorName = operatorName;
        }
    }

    public static class InputChannelSpecification extends Payload
    {
        public int inputChannel;

        public InputChannelSpecification(int inputChannel)
        {
            this.inputChannel = inputChannel;
        }
    }

    public static class SequenceNumber extends Payload
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

        public void assignSubsequence(SequenceNumber sequenceNumber)
        {
            this.subsequenceNumber = sequenceNumber;
        }
    }

    public static class EndOfOutput extends Payload
    {

    }

    public void addPayload(Payload p)
    {
        if (p instanceof MeantFor)
            payloads.put(MessageTypes.MEANT_FOR, p);
        else if (p instanceof InputChannelSpecification)
            payloads.put(MessageTypes.INPUT_CHANNEL, p);
        else if (p instanceof SequenceNumber)
            payloads.put(MessageTypes.SEQUENCE_NUMBER, p);
        else if (p instanceof EndOfOutput)
            payloads.put(MessageTypes.END_OF_OUTPUT, p);
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
}