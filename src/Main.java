import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Semaphore;

public class Main {
    public static void main(String[] args) {
        Instant start = Instant.now();

        Runtime runtime = Runtime.getRuntime();
        String path = "D:\\workspace\\java-threads\\untitled\\large_dataset_1M.csv";
        File largeFile = new File(path);
        Semaphore semaphore = new Semaphore(1);
        List<Thread> threads = new ArrayList<>();

        try {
            List<File> files = splitFile(
                    largeFile,
                    (int)(largeFile.length() / runtime.availableProcessors())
            );
            int count = 1;
            for(File file : files){
                Reader reader = new Reader(file, count);
                reader.start();
                threads.add(reader);
                count++;
            }

            threads.forEach(t -> {
                try {
                    t.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });

        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            Instant finish = Instant.now();
            long timeElapsed = Duration.between(start, finish).toMillis();
            System.out.println("Tempo de total do programa: " + timeElapsed);
        }

    }

    public static List<File> splitFile(File file, int sizeOfChunk) throws IOException {
        int counter = 1;
        List<File> files = new ArrayList<File>();
        String eof = System.lineSeparator();
        try (BufferedInputStream bi = new BufferedInputStream(new FileInputStream(file))) {
            String name = file.getName();
            byte [] bytes = new byte[sizeOfChunk];
            int dataRead = bi.read(bytes);
            while (dataRead > 0) {
                File newFile = new File(file.getParent(), name + "."
                        + String.format("%03d", counter++));
                try (OutputStream out = new BufferedOutputStream(new FileOutputStream(newFile))) {
                    int i = 1;
                    System.out.println(String.valueOf(new String(bytes, StandardCharsets.UTF_8).charAt(new String(bytes, StandardCharsets.UTF_8).length() - 1)).equals(eof));
                    while(!String.valueOf(new String(bytes, StandardCharsets.UTF_8).charAt(new String(bytes, StandardCharsets.UTF_8).length() - 1)).equals(eof)){
                        bytes = new byte[sizeOfChunk - i];
                        System.out.println(bytes.length);
                        dataRead = bi.read(bytes);
                        i++;
                    }
                    out.write(bytes);
                }
                files.add(newFile);
            }
        }
        return files;
    }
}