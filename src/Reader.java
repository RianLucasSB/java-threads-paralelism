import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Date;
import java.util.Locale;

public class Reader extends Thread{
    private String file;
    private int threadId;

    public Reader(String file, int threadId) {
        this.file = file;
        this.threadId = threadId;
    }

    @Override
    public void run(){
        Instant start = Instant.now();

        Runtime runtime = Runtime.getRuntime();
        BufferedReader br = null;
        String line = "";
        String csvDivisor = ",";
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            br = new BufferedReader(new FileReader(file));
            while ((line = br.readLine()) != null) {
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

                System.out.println(country);
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        } finally {
            Instant finish = Instant.now();
            long timeElapsed = Duration.between(start, finish).toMillis();
            System.out.println("Tempo de processameto da thread " + threadId + ": " + timeElapsed);
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
