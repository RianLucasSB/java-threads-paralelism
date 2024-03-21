import java.io.*;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        Instant start = Instant.now();

        Runtime runtime = Runtime.getRuntime();
        String path = "D:\\workspace\\java-threads\\untitled\\large_dataset_5M.csv";
        File largeFile = new File(path);
        List<Thread> threads = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(largeFile))) {
            long fileSize = largeFile.length();
            long chunkSize = fileSize / (runtime.availableProcessors() - 1);

            String line;
            int chunkCount = 0;
            long bytesRead = 0;
            reader.readLine();
            String chunkFileName = "chunk_" + (chunkCount++) + ".csv";
            BufferedWriter writer = new BufferedWriter(new FileWriter(chunkFileName));
            while ((line = reader.readLine()) != null) {
                if (bytesRead + line.getBytes().length >= chunkSize) {
                    writer.close();

                    Reader thread = new Reader(chunkFileName, chunkCount);

                    chunkFileName = "chunk_" + (chunkCount++) + ".csv";
                    writer = new BufferedWriter(new FileWriter(chunkFileName));
                    bytesRead = 0;

                    threads.add(thread);// Process the chunk in a thread
                }
                writer.write(line);
                writer.newLine();
                bytesRead += line.getBytes().length;

            }

            writer.close();

            for(Thread thread : threads){
                thread.start();
            }

            for(Thread thread : threads){
                thread.join();
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            Instant finish = Instant.now();
            long timeElapsed = Duration.between(start, finish).toMillis();
            System.out.println("Tempo de total do programa: " + timeElapsed + "ms");
        }
    }
}