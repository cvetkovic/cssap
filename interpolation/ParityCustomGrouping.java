package cssap;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import java.util.Collections;
import java.util.List;

public class ParityCustomGrouping implements CustomStreamGrouping
{
    private List<Integer> listOfTargets;

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks)
    {
        listOfTargets = targetTasks;
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values)
    {
        int orderNumber = (Integer)values.get(2);

        return Collections.singletonList(listOfTargets.get(orderNumber % 2));
    }
}