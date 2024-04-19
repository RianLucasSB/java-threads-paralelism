import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ParallelFileReader {
    private static final int BUFFER_SIZE = 50000000;
    private static final int NUM_THREADS = Runtime.getRuntime().availableProcessors() - 1;
    private static final Lock channelLock = new ReentrantLock();
    private static final Map<String, Integer> transactionsByCountryFinalResult = new ConcurrentHashMap<>();
    private static final Map<String, Double> totalSalesByProduct = new ConcurrentHashMap<>();
    private static final Map<String, Integer> totalSalesByProductQuantity = new ConcurrentHashMap<>();
    private static final Map<String, Integer> transactionsByUser = new ConcurrentHashMap<>();
    private static final Map<String, Double> totalSpendingByUser = new ConcurrentHashMap<>();
    private static final Map<String, Integer> salesByCompanyFinalResult = new ConcurrentHashMap<>();
    private static final Map<String, Integer> transactionsByPaymentMethodFinalResult = new ConcurrentHashMap<>();

    private static final Map<String, Integer> salesByMonthYearFinalResult = new ConcurrentHashMap<>();
    private static final Map<String, Double> salesByCurrencyFinalResult = new ConcurrentHashMap<>();
    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();

        File file = new File("large_dataset_5M.csv");

        long fileSize = file.length();

        long chunkSize = fileSize / NUM_THREADS;

        List<Thread> threads = new ArrayList<>();

        System.out.println(chunkSize);

        try (RandomAccessFile raf = new RandomAccessFile(file, "r");
             FileChannel channel = raf.getChannel()) {

            // Skip the first line (labels)
            long start = findNextLine(channel, 0);

            for (int i = 0; i < NUM_THREADS; i++) {
                long end = (i == NUM_THREADS - 1) ? fileSize : (i + 1) * chunkSize;

                end = findNearestEOL(channel, end);

                Thread thread = new Thread(new CSVReaderThread(start, end, channel, i + 1));
                threads.add(thread);

                start = end + 1;
            }

            for (Thread thread : threads) {
                thread.start();
            }

            // Wait for all threads to finish
            for (Thread thread : threads) {
                thread.join();
            }


            System.out.println("Transações por país: ----------------------------------");
            for (Map.Entry<String, Integer> entry : transactionsByCountryFinalResult.entrySet()) {
                System.out.println(entry.getKey() + ": " + entry.getValue());
            }

            System.out.println("Media por produto: ----------------------------------");
            for (Map.Entry<String, Integer> entry : totalSalesByProductQuantity.entrySet()) {
                System.out.println(entry.getKey() + ": " + totalSalesByProduct.get(entry.getKey()) / entry.getValue());
            }

            System.out.println("Total de vendas por empresa: ----------------------------------");
            for (Map.Entry<String, Integer> entry : salesByCompanyFinalResult.entrySet()) {
                System.out.println(entry.getKey() + ": " + entry.getValue());
            }

            System.out.println("Quantidade de transações por método de pagamento: ----------------------------------");
            for (Map.Entry<String, Integer> entry : transactionsByPaymentMethodFinalResult.entrySet()) {
                System.out.println(entry.getKey() + ": " + entry.getValue());
            }

            System.out.println("Distribuição de vendas por mês/ano: ----------------------------------");
            for (Map.Entry<String, Integer> entry : salesByMonthYearFinalResult.entrySet()) {
                System.out.println(entry.getKey() + ": " + entry.getValue());
            }

            System.out.println("Média de gastos por usuário: ----------------------------------");
            for (Map.Entry<String, Integer> entry : transactionsByUser.entrySet()) {
                System.out.println(entry.getKey() + ": " + totalSpendingByUser.get(entry.getKey()) / entry.getValue());

            }


            System.out.println("Total de vendas em cada moeda: ----------------------------------");
            for (Map.Entry<String, Double> entry : salesByCurrencyFinalResult.entrySet()) {
                System.out.println(entry.getKey() + ": " + entry.getValue());
            }

            long endTime = System.currentTimeMillis();
            long executionTime = endTime - startTime;
            System.out.println("Program execution time: " + executionTime + "ms");
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    static long findNearestEOL(FileChannel channel, long position) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(1);

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
            channelLock.unlock();
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
            channelLock.lock();
            while (true) {
                buffer.clear();
                channel.position(position);
                bytesRead = channel.read(buffer);
                if (bytesRead == -1) {
                    return channel.size();
                }

                buffer.flip();
                for (int i = 0; i < buffer.limit(); i++) {
                    if (buffer.get(i) == '\n') {
                        return position + i + 1;
                    }
                }
                position += bytesRead;
            }
        } finally {
            channelLock.unlock();
        }
    }

    static class CSVReaderThread implements Runnable {
        private long start;
        private long end;
        private FileChannel channel;
        private int batchSize = 1000;
        private int i;


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
                // Create a ByteBuffer for reading data
                StringBuilder lineBuilder = new StringBuilder();
                String csvDivisor = ",";
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
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

                    buffer.flip();
                    List<String> batchLines = new ArrayList<>();
                    while (buffer.hasRemaining()) {
                        char c = (char) buffer.get();
                        if (c == '\n') {
                            batchLines.add(lineBuilder.toString());
                            lineBuilder.setLength(0);
                        } else {
                            lineBuilder.append(c);
                        }
                    }
                    buffer.clear();
                    processBatch(batchLines, formatter);
                }


                long endTime = System.currentTimeMillis();
                long executionTime = endTime - startTime;
                System.out.println("Thread: " + i + " execution time: " + executionTime + "ms");

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private void processBatch(List<String> batchLines, DateTimeFormatter formatter) {
            // Process each line in the batch
            for (String line : batchLines) {
                // Parse CSV line
                String[] transaction = line.split(",");

                String transactionId = transaction[0];
                String userId = transaction[1];
                String companyId = transaction[2];
                LocalDateTime transactionDate = LocalDateTime.parse(transaction[3], formatter);
                String productId = transaction[4];
                String productDescription = transaction[5];
                int quantity = Integer.parseInt(transaction[6]);
                Double pricePerUnit = Double.parseDouble(transaction[7]);
                String currency = transaction[8];
                String paymentMethod = transaction[9];
                String country = transaction[10];
                String city = transaction[11];


                transactionsByCountryFinalResult.compute(country, (k, v) -> {
                    if (v != null) {
                        return 1 + v;
                    } else {
                        return 1;
                    }
                });

                salesByCompanyFinalResult.compute(companyId, (k, v) -> {
                    if (v != null) {
                        return 1 + v;
                    } else {
                        return 1;
                    }
                });

                transactionsByPaymentMethodFinalResult.compute(paymentMethod, (k, v) -> {
                    if (v != null) {
                        return 1 + v;
                    } else {
                        return 1;
                    }
                });

                String monthYear = transactionDate.getMonthValue() + "-" + transactionDate.getYear();
                salesByMonthYearFinalResult.compute(monthYear, (k, v) -> {
                    if (v != null) {
                        return 1 + v;
                    } else {
                        return 1;
                    }
                });

                totalSalesByProduct.compute(productId, (k, v) -> {
                    if (v != null) {
                        return pricePerUnit + v;
                    } else {
                        return pricePerUnit;
                    }
                });

                totalSalesByProductQuantity.compute(productId, (k, v) -> {
                    if (v != null) {
                        return 1 + v;
                    } else {
                        return 1;
                    }
                });

                totalSpendingByUser.compute(userId, (k, v) -> {
                    if (v != null) {
                        return (pricePerUnit * quantity) + v;
                    } else {
                        return (pricePerUnit * quantity);
                    }
                });

                transactionsByUser.compute(productId, (k, v) -> {
                    if (v != null) {
                        return 1 + v;
                    } else {
                        return 1;
                    }
                });

                double totalPrice = pricePerUnit * quantity;
                salesByCurrencyFinalResult.compute(currency, (k, v) -> {
                    if (v != null) {
                        return totalPrice + v;
                    } else {
                        return totalPrice;
                    }
                });
            }
        }

    }
}
