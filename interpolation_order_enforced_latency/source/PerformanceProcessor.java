package cvetkovic;

import java.io.*;

public class PerformanceProcessor
{
    public static void main(String[] args) throws Exception
    {
        File folder = new File("C:\\Users\\cl160127d\\AppData\\Local\\Packages\\CanonicalGroupLimited.UbuntuonWindows_79rhkp1fndgsc\\LocalState\\rootfs\\home\\cvetkovic");

        for (File fileEntry : folder.listFiles())
        {
            if (!fileEntry.isDirectory())
            {
                String fileName = fileEntry.getName();
                if (!fileName.startsWith("performance"))
                    continue;

                double runningSum = 0;
                int numberOfLines = 0;

                System.out.println("-----------------------------------------------");
                System.out.println("-------------- " + fileName + " --------------");

                try(BufferedReader bufferedReader = new BufferedReader(new FileReader(fileEntry)))
                {
                    String line;

                    while ((line = bufferedReader.readLine()) != null)
                    {
                        runningSum += Double.parseDouble(line);
                        numberOfLines++;
                    }
                }

                double average = runningSum / numberOfLines;
                double std = 0;

                try(BufferedReader bufferedReader = new BufferedReader(new FileReader(fileEntry)))
                {
                    String line;

                    while ((line = bufferedReader.readLine()) != null)
                    {
                        std = Math.pow((average - Double.parseDouble(line)), 2);
                    }
                }

                std /= numberOfLines;
                std = Math.sqrt(std);

                System.out.println("Mean:\t\t\t\t" + average + " ms");
                System.out.println("Standard deviation:\t" + std + " ms");
                System.out.println("-----------------------------------------------");
                System.out.println("");

                try (BufferedWriter bufferedWriter =
                             new BufferedWriter(new FileWriter("C:\\Users\\cl160127d\\Desktop\\mean.txt", true)))
                {
                    bufferedWriter.write(average + "\n");
                }

                try (BufferedWriter bufferedWriter =
                             new BufferedWriter(new FileWriter("C:\\Users\\cl160127d\\Desktop\\std.txt", true)))
                {
                    bufferedWriter.write(std + "\n");
                }
            }
        }
    }
}