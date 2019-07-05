import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import java.util.Collections;
import java.util.List;

public class SourceToSplitterRouting implements CustomStreamGrouping
{
    private List<Integer> listOfCounters;
    private int numberOfTargetTasks;

    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks)
    {
        numberOfTargetTasks = targetTasks.size();
        listOfCounters = targetTasks;
    }

    public List<Integer> chooseTasks(int taskId, List<Object> values)
    {
        String receivedText = "";

        if (values.get(0) != null)
            receivedText = (String)values.get(0);
        boolean endOfStreamMarker = (boolean)values.get(1);

        if (endOfStreamMarker)
            return listOfCounters;
        else
        {
            int hash = receivedText.hashCode();
            if (hash < 0)
                hash *= -1;

            int sendTo = hash % numberOfTargetTasks;

            return Collections.singletonList(listOfCounters.get(sendTo));
        }
    }
}