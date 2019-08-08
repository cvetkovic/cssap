package compiler.storm;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class SystemMessage implements Serializable
{
    public enum MessageTypes
    {
        MEANT_FOR(0),
        INPUT_CHANNEL(1);
        // WHEN ADDING NEW MESSAGE TYPES CHANGE RETURNED VALUE IN count() METHOD

        private final int i;

        MessageTypes(int i)
        {
            this.i = i;
        }

        private static int count()
        {
            return 2;
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

    public void addPayload(Payload p)
    {
        if (p instanceof MeantFor)
            payloads.put(MessageTypes.MEANT_FOR, p);
        else if (p instanceof InputChannelSpecification)
            payloads.put(MessageTypes.INPUT_CHANNEL, p);
        else
            throw new RuntimeException("Not supported type of payload.");
    }

    public Payload getPayloadByType(MessageTypes type)
    {
        return payloads.get(type);
    }
}