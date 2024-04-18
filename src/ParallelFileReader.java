import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class ParallelFileReader {
    private static final int BUFFER_SIZE = 50000000; // Adjust as needed
    private static final int NUM_THREADS = Runtime.getRuntime().availableProcessors() - 1; // Number of available processors
    private static final Lock channelLock = new ReentrantLock();

    private static final HashMap<String, List<String>> transactionsPerCountryResult = new HashMap<>();
    // Lock for synchronizing access to FileChannel
    public static void main(String[] args) {
        File file = new File("large_dataset_5M.csv");

        // Get file size
        long fileSize = file.length();

        // Calculate chunk size for each thread
        long chunkSize = fileSize / NUM_THREADS;

        // Create threads list
        List<Thread> threads = new ArrayList<>();

        try (RandomAccessFile raf = new RandomAccessFile(file, "r");
             FileChannel channel = raf.getChannel()) {

            // Skip the first line (labels)
            long start = findNextLine(channel, 0);

            for (int i = 0; i < NUM_THREADS; i++) {
                // Calculate end position for each thread
                long end = (i == NUM_THREADS - 1) ? fileSize : (i + 1) * chunkSize;

                // Find the nearest EOL before the calculated end position
                end = findNearestEOL(channel, end);

                // Create a new thread for processing
                Thread thread = new Thread(new CSVReaderThread(start, end, channel, i + 1));
                threads.add(thread);
                thread.start();

                // Move the start position to the next line
                start = end + 1;
            }

            // Wait for all threads to finish
            for (Thread thread : threads) {
                thread.join();
            }

            for (Map.Entry<String, List<String>> entry : transactionsPerCountryResult.entrySet()) {
                System.out.println(entry.getKey() + ": " + entry.getValue().size());
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    static long findNearestEOL(FileChannel channel, long position) throws IOException {
        // Create a ByteBuffer to read data
        ByteBuffer buffer = ByteBuffer.allocate(1);

        // Read backwards until an EOL character is found
        try {
            channelLock.lock();
        while (position > 0) {
            position--;
            channel.position(position);
            channel.read(buffer);
            buffer.flip();
            if (buffer.get(0) == '\n') {
                break;
            }
            buffer.clear();
        }
        } finally {
            channelLock.unlock(); // Release the lock in the finally block to ensure it's always released
        }

        // If the loop exited because of an EOL, return the position
        // If the loop exited because the start of the file was reached, return position 0
        return (position == 0) ? position : position + 1;
    }

    static long findNextLine(FileChannel channel, long position) throws IOException {
        // Read data in chunks until a newline character is found
        ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
        int bytesRead;

        try {
            channelLock.lock(); // Acquire the lock
            // Read until an EOL character is found
            while (true) {
                buffer.clear();
                channel.position(position);
                bytesRead = channel.read(buffer);
                if (bytesRead == -1) {
                    // End of file reached, return the file size
                    return channel.size();
                }

                // Search for the next EOL character in the buffer
                buffer.flip();
                for (int i = 0; i < buffer.limit(); i++) {
                    if (buffer.get(i) == '\n') {
                        // Found the next line, return the position after the newline character
                        return position + i + 1;
                    }
                }
                position += bytesRead;
            }
        } finally {
            channelLock.unlock(); // Release the lock in the finally block to ensure it's always released
        }
    }

    static class CSVReaderThread implements Runnable {
        private long start;
        private long end;
        private FileChannel channel;

        private int i;

        HashMap<String, List<String>> transactionsPerCountry = new HashMap<>();


        public CSVReaderThread(long start, long end, FileChannel channel, int i) {
            this.start = start;
            this.end = end;
            this.channel = channel;
            this.i = i;
        }

        @Override
        public void run() {
            try {
                long startTime = System.currentTimeMillis();
                System.out.printf("thread %d started %d%n", i, startTime);
                System.out.println();
                // Create a ByteBuffer for reading data
                StringBuilder lineBuilder = new StringBuilder();
                String csvDivisor = ",";
                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                // Move to the start position
                long bytesRead = 0;
                long totalBytes = end - start;
                // Read until the end position
                while (bytesRead < totalBytes) {
                    // Read data into the buffer
                    channelLock.lock();
                    channel.position(start + bytesRead);
                    ByteBuffer buffer = ByteBuffer.allocate((int) Math.min((totalBytes - bytesRead), BUFFER_SIZE));

                    int read = channel.read(buffer);
                    channelLock.unlock();
                    if (read == -1) {
                        break; // End of file
                    }
                    bytesRead += read;

                    // Process the data (parse CSV lines)
                    buffer.flip();
                    while (buffer.hasRemaining()) {
                        char c = (char) buffer.get();
                        if (c == '\n') {
                            // End of line reached, parse the line
                            String line = lineBuilder.toString();
                            // Process the CSV line (you can implement your CSV parsing logic here)
                            String[] transaction = line.split(csvDivisor);

                            String transactionId = transaction[0];
                            String userId = transaction[1];
                            String companyId = transaction[2];
                            Date transactionDate = formatter.parse(transaction[3]);
                            String productId = transaction[4];
                            String productDescription = transaction[5];
                            int quantity = Integer.parseInt(transaction[6]);
                            Double pricePerUnit = Double.parseDouble(transaction[7]);
                            String currency = transaction[8];
                            String paymentMethod = transaction[9];
                            String country = transaction[10];
                            String city = transaction[11];

                            if(transactionsPerCountry.containsKey(country)){
                                transactionsPerCountry.get(country).add(transactionId);
                            } else {
                                transactionsPerCountry.put(country, new ArrayList<>(List.of(transactionId)));
                            }
                            // Reset the StringBuilder for the next line
                            lineBuilder.setLength(0);
                        } else {
                            lineBuilder.append(c);
                        }
                    }
                    buffer.clear();
                }

                channelLock.lock();
                for (Map.Entry<String, List<String>> entry : transactionsPerCountry.entrySet()) {
                    if(transactionsPerCountryResult.containsKey(entry.getKey())){
                        transactionsPerCountryResult.get(entry.getKey()).addAll(entry.getValue());
                    } else {
                        transactionsPerCountryResult.put(entry.getKey(), (entry.getValue()));
                    }
                }
                channelLock.unlock();
                long endTime = System.currentTimeMillis();
                long executionTime = endTime - startTime;
                System.out.println("Thread: " + i + " execution time: " + executionTime + "ms");

            } catch (IOException e) {
                e.printStackTrace();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
