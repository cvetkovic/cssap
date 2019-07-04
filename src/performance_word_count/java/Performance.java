import java.io.BufferedWriter;
import java.io.FileWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Performance
{
    public synchronized static void WriteTimestampToFile(Date date)
    {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("C:\\Users\\cl160127d\\Desktop\\perforamnce.txt", true)))
        {
            writer.newLine();
            writer.write(Long.toString(date.getTime()));
        }
        catch (Exception ex)
        {

        }
    }

    public synchronized static void WriteTimestampToFile(String value)
    {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("C:\\Users\\cl160127d\\Desktop\\perforamnce.txt", true)))
        {
            writer.newLine();
            writer.write(value);
        }
        catch (Exception ex)
        {

        }
    }
}