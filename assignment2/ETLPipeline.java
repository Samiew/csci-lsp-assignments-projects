import java.io.*;
import java.util.*;

public class ETLPipeline {
    public static void main(String[] args) {
        String inputFile = "input.txt";
        String outputFile = "output.txt";

        try {
            BufferedReader reader = new BufferedReader(new FileReader(inputFile));
            BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));

            String line;

            while ((line = reader.readLine()) != null) {
                String transformed = line.toUpperCase();
                writer.write(transformed);
                writer.newLine();
            }

            reader.close();
            writer.close();

            System.out.println("ETL process completed successfully.");

        } catch (IOException e) {
            System.out.println("Error: " + e.getMessage());
        }
    }
}