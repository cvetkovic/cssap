package compiler.storm.groupings;

import compiler.storm.SystemMessage;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class MultipleOutputGrouping implements CustomStreamGrouping
{
    private List<Integer> targetTasks;
    private String operatorName;
    private static List<Integer> emptyList = new LinkedList<>();

    public MultipleOutputGrouping(String operatorName)
    {
        this.operatorName = operatorName;
    }

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks)
    {
        this.targetTasks = targetTasks;
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values)
    {
        SystemMessage systemMessage = (SystemMessage) values.get(1);
        if (systemMessage == null)
            return targetTasks;

        SystemMessage.Payload payload = ((SystemMessage) values.get(1)).getPayloadByType(SystemMessage.MessageTypes.MEANT_FOR);

        if (payload != null)
        {
            if (((SystemMessage.MeantFor)payload).operatorName.equals(operatorName))
                if (targetTasks.contains(((SystemMessage.MeantFor)payload).tupleMeantFor))
                    return Collections.singletonList(((SystemMessage.MeantFor)payload).tupleMeantFor);
                else
                    return emptyList;
        }

        return targetTasks;
    }
}