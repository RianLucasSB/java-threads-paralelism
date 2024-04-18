import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class ParallelFileReader {
    private static final int BUFFER_SIZE = 5000000; // Adjust as needed
    private static final int NUM_THREADS = Runtime.getRuntime().availableProcessors() - 1; // Number of available processors
    private static final Lock channelLock = new ReentrantLock();

    // Total de transações por país
    private static final Map<String, Integer> transactionsByCountryFinalResult = new HashMap<>();
    private static final Map<String, Double> avgPriceByProductFinalResult = new HashMap<>();
    private static final Map<String, Integer> salesByCompanyFinalResult = new HashMap<>();
    private static final Map<String, Integer> transactionsByPaymentMethodFinalResult = new HashMap<>();

    private static final Map<String, Double> avgSpendingByUserFinalResult = new HashMap<>();

    private static final Map<String, Integer> salesByMonthYearFinalResult = new HashMap<>();
    private static final Map<String, Integer> commonTransactionsByCityFinalResult = new HashMap<>();
    private static final Map<String, Double> salesByCurrencyFinalResult = new HashMap<>();
    public static void main(String[] args) {
        File file = new File("large_dataset_1M.csv");

        long fileSize = file.length();

        long chunkSize = fileSize / NUM_THREADS;

        List<Thread> threads = new ArrayList<>();

        try (RandomAccessFile raf = new RandomAccessFile(file, "r");
             FileChannel channel = raf.getChannel()) {

            // Skip the first line (labels)
            long start = findNextLine(channel, 0);

            for (int i = 0; i < NUM_THREADS; i++) {
                long end = (i == NUM_THREADS - 1) ? fileSize : (i + 1) * chunkSize;

                end = findNearestEOL(channel, end);

                Thread thread = new Thread(new CSVReaderThread(start, end, channel, i + 1));
                threads.add(thread);
                thread.start();

                start = end + 1;
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
            for (Map.Entry<String, Double> entry : avgPriceByProductFinalResult.entrySet()) {
                System.out.println(entry.getKey() + ": " + entry.getValue());
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
            for (Map.Entry<String, Double> entry : avgSpendingByUserFinalResult.entrySet()) {
                System.out.println(entry.getKey() + ": " + entry.getValue());
            }


            System.out.println("Total de vendas em cada moeda: ----------------------------------");
            for (Map.Entry<String, Double> entry : salesByCurrencyFinalResult.entrySet()) {
                System.out.println(entry.getKey() + ": " + entry.getValue());
            }
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

        private int i;

        Map<String, Integer> transactionsByCountry = new HashMap<>();
        Map<String, Double> totalSalesByProduct = new HashMap<>();
        Map<String, Integer> totalSalesByProductQuantity = new HashMap<>();
        Map<String, Integer> salesByCompany = new HashMap<>();
        Map<String, Integer> transactionsByPaymentMethod = new HashMap<>();
        Map<String, Integer> transactionsByUser = new HashMap<>();
        Map<String, Integer> salesByMonthYear = new HashMap<>();
        Map<String, Integer> commonTransactionsByCity = new HashMap<>();
        Map<String, Double> totalSpendingByUser = new HashMap<>();
        Map<String, Double> salesByCurrency = new HashMap<>();


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

                    // Process the data (parse CSV lines)
                    buffer.flip();
                    while (buffer.hasRemaining()) {
                        char c = (char) buffer.get();
                        if (c == '\n') {
                            // End of line reached, parse the line
                            String line = lineBuilder.toString();
                            // Process the CSV line
                            String[] transaction = line.split(csvDivisor);

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


                            transactionsByCountry.put(country, transactionsByCountry.getOrDefault(country, 0) + 1);

                            salesByCompany.put(companyId, salesByCompany.getOrDefault(companyId, 0) + 1);

                            transactionsByPaymentMethod.put(paymentMethod, transactionsByPaymentMethod.getOrDefault(paymentMethod, 0) + 1);

                            String monthYear = transactionDate.getMonthValue() + "-" + transactionDate.getYear();
                            salesByMonthYear.put(monthYear, salesByMonthYear.getOrDefault(monthYear, 0) + 1);

                            commonTransactionsByCity.put(city, commonTransactionsByCity.getOrDefault(city, 0) + 1);

                            double totalPrice = pricePerUnit * quantity;
                            salesByCurrency.put(currency, salesByCurrency.getOrDefault(currency, 0.0) + totalPrice);

                            totalSalesByProduct.put(productId, totalSalesByProduct.getOrDefault(currency, 0.0) + pricePerUnit);

                            totalSalesByProductQuantity.put(productId, totalSalesByProductQuantity.getOrDefault(currency, 0) + 1);

                            totalSpendingByUser.put(userId, totalSpendingByUser.getOrDefault(userId, 0.0) + totalPrice);

                            transactionsByUser.put(userId, transactionsByUser.getOrDefault(userId, 0) + 1);

                            lineBuilder.setLength(0);
                        } else {
                            lineBuilder.append(c);
                        }
                    }
                    buffer.clear();
                }


                channelLock.lock();
                for (Map.Entry<String, Integer> entry : transactionsByCountry.entrySet()) {
                        transactionsByCountryFinalResult.put(entry.getKey(), transactionsByCountryFinalResult.getOrDefault(entry.getKey(), 0) + entry.getValue());
                }

                for (Map.Entry<String, Integer> entry : totalSalesByProductQuantity.entrySet()) {
                    avgPriceByProductFinalResult.put(entry.getKey(), avgPriceByProductFinalResult.getOrDefault(entry.getKey(), 0.0) + (totalSalesByProduct.get(entry.getKey()) /  entry.getValue()));
                }

                for (Map.Entry<String, Integer> entry : salesByCompany.entrySet()) {
                    salesByCompanyFinalResult.put(entry.getKey(), salesByCompanyFinalResult.getOrDefault(entry.getKey(), 0) + entry.getValue());
                }

                for (Map.Entry<String, Integer> entry : transactionsByPaymentMethod.entrySet()) {
                    transactionsByPaymentMethodFinalResult.put(entry.getKey(), transactionsByPaymentMethodFinalResult.getOrDefault(entry.getKey(), 0) + entry.getValue());
                }

                for (Map.Entry<String, Integer> entry : salesByMonthYear.entrySet()) {
                    salesByMonthYearFinalResult.put(entry.getKey(), salesByMonthYearFinalResult.getOrDefault(entry.getKey(), 0) + entry.getValue());
                }

                for (Map.Entry<String, Integer> entry : transactionsByUser.entrySet()) {
                    avgSpendingByUserFinalResult.put(entry.getKey(), avgSpendingByUserFinalResult.getOrDefault(entry.getKey(), 0.0) + (totalSpendingByUser.get(entry.getKey()) /  entry.getValue()));
                }

                for (Map.Entry<String, Double> entry : salesByCurrency.entrySet()) {
                    salesByCurrencyFinalResult.put(entry.getKey(), salesByCurrencyFinalResult.getOrDefault(entry.getKey(), 0.0) + entry.getValue());
                }

                channelLock.unlock();
                long endTime = System.currentTimeMillis();
                long executionTime = endTime - startTime;
                System.out.println("Thread: " + i + " execution time: " + executionTime + "ms");

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
