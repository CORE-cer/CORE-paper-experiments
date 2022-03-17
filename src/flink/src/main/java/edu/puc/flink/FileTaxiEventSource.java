package edu.puc.flink;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;

public class FileTaxiEventSource extends RichParallelSourceFunction<Event> {
    private final String fileName;
    public static int events = 0;


    FileTaxiEventSource(String streamFileName){
        fileName = streamFileName;
    }

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        if (!Taxi.memoryTest) {
            try {
                int timeout = Taxi.timeout;
                FileReader file = new FileReader(fileName);
                BufferedReader reader = new BufferedReader(file);
                LinkedList<Event> eventList = new LinkedList<>();
                long start = System.nanoTime();
                String line;

                while ((line = reader.readLine()) != null){
                    TaxiEvent event = TaxiEvent.getEventFromString(line);
                    eventList.add(event);
                }

                (new Thread(() -> {
                    try {
                        Thread.sleep(30000);
                        System.out.print((double)(System.nanoTime() - start)/1000000000 + ",");
                        System.out.print(events + ",");
                        System.out.print((double)Taxi.enumerationTime/1000000000 + ",");
                        System.out.print(Taxi.totalResults);
                        System.out.println();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                })).start();

                while (!eventList.isEmpty()) {
                    events++;
                    Event event = eventList.removeFirst();
                    ctx.collect(event);
                    if (timeout != 0 && System.nanoTime() - start >= timeout * 1000000000L) {
                        break;
                    }
                }
                ctx.close();

                try {
                    reader.close();
                }
                catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
            catch (FileNotFoundException | NullPointerException ex){
                ex.printStackTrace();
            }}
        else {
            try {
                int timeout = Taxi.timeout;
                FileReader file = new FileReader(fileName);
                BufferedReader reader = new BufferedReader(file);
                long start = System.nanoTime();
                String line;

                long total = 0;
                long tmp;


                (new Thread(() -> {
                    try {
                        Thread.sleep(timeout * 1000L);
                        if (Taxi.count == 0) {
                            Taxi.count = 1;
                        }
                        System.out.print(Taxi.maxMemTotal + ",");
                        System.out.print(Taxi.avgMemTotal/Taxi.count + ",");
                        System.out.print(Taxi.maxMemUsed + ",");
                        System.out.println(Taxi.avgMemUsed/Taxi.count);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                })).start();
                while ((line = reader.readLine()) != null) {
                    if (Main.maxEvents != 0 && events >= Main.maxEvents) {
                        break;
                    }
                    TaxiEvent event = TaxiEvent.getEventFromString(line);
                    tmp = System.nanoTime();
                    ctx.collect(event);
                    total += System.nanoTime() - tmp;
                    events++;
                    if (events % 10000 == 0) {
                        Taxi.avgMemTotal = Runtime.getRuntime().totalMemory();
                        Taxi.avgMemTotal += Taxi.avgMemTotal;
                        if (Taxi.avgMemTotal > Taxi.maxMemTotal) {
                            Taxi.maxMemTotal = Taxi.avgMemTotal;
                        }
                        System.gc();
                        Taxi.avgMemUsed = Taxi.avgMemTotal - Runtime.getRuntime().freeMemory();
                        Taxi.avgMemUsed += Taxi.avgMemUsed;
                        if (Taxi.avgMemUsed > Taxi.maxMemUsed) {
                            Taxi.maxMemUsed = Taxi.avgMemUsed;
                        }
                        Taxi.count++;
                    }
                    if (timeout != 0 && System.nanoTime() - start >= timeout * 1000000000L) {
                        //                    System.err.println(event.getId());
                        break;
                    }
                }


                ctx.close();

                //        System.out.print((((double)compileTime) / 1000000000) + ",");
                //        System.out.print((((double)total) / 1000000000) + ",");
                //        System.out.print((((double)EventListener.totalTime) / 1000000000) + ",");
                //        System.out.println(EventListener.totalMatches);

                //            System.out.println("context collection time: " + (total / 1000000));
                Taxi.executionTime = total;
                try {
                    reader.close();
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            } catch (FileNotFoundException | NullPointerException ex) {
                ex.printStackTrace();
            }
        }
    }

    @Override
    public void cancel() { }
}