package edu.puc.flink;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class FileStockEventSource extends RichParallelSourceFunction<Event> {
    private final String fileName;
    public static int events = 0;


    FileStockEventSource(String streamFileName){
        fileName = streamFileName;
    }

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        try {
            FileReader file = new FileReader(fileName);
            BufferedReader reader = new BufferedReader(file);
            long start = System.nanoTime();
            String line;

            long total = 0;
            long tmp;
            while ((line = reader.readLine()) != null){
                long t0 = System.nanoTime();
                StockEvent event = StockEvent.getEventFromString(line);
                tmp = System.nanoTime();
                ctx.collect(event);
                total += System.nanoTime() - tmp;
                events++;
                if (System.nanoTime() - start >= 30000000000L) {
                    break;
                }
            }

            ctx.close();

    //        System.out.print((((double)compileTime) / 1000000000) + ",");
    //        System.out.print((((double)total) / 1000000000) + ",");
    //        System.out.print((((double)EventListener.totalTime) / 1000000000) + ",");
    //        System.out.println(EventListener.totalMatches);

//            System.out.println("context collection time: " + (total / 1000000));
            Stock.executionTime = total;
            try {
                reader.close();
            }
            catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        catch (FileNotFoundException | NullPointerException ex){
            ex.printStackTrace();
        }
    }

    @Override
    public void cancel() { }
}