package edu.puc.flink;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;

public class FileSmartHomesEventSource extends RichParallelSourceFunction<Event> {
    private final String fileName;
    public static int events = 0;


    FileSmartHomesEventSource(String streamFileName){
        fileName = streamFileName;
    }

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        if (!Smart.memoryTest) {
            try {
                int timeout = Smart.timeout;
                FileReader file = new FileReader(fileName);
                BufferedReader reader = new BufferedReader(file);
                LinkedList<Event> eventList = new LinkedList<>();
                long start = System.nanoTime();
                String line;

                while ((line = reader.readLine()) != null){
                    SmartHomesEvent event = SmartHomesEvent.getEventFromString(line);
                    eventList.add(event);
                }

                (new Thread(() -> {
                    try {
                        Thread.sleep(30000);
                        System.out.print((double)(System.nanoTime() - start)/1000000000 + ",");
                        System.out.print(FileSmartHomesEventSource.events + ",");
                        System.out.print((double)Smart.enumerationTime/1000000000 + ",");
                        System.out.print(Smart.totalResults);
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
            }
        }
        else {
            try {
                int timeout = Smart.timeout;
                FileReader file = new FileReader(fileName);
                BufferedReader reader = new BufferedReader(file);
                long start = System.nanoTime();
                String line;

                long total = 0;
                long tmp;


                (new Thread(() -> {
                    try {
                        Thread.sleep(timeout * 1000L);
                        if (Smart.count == 0) {
                            Smart.count = 1;
                        }
                        System.out.print(Smart.maxMemTotal + ",");
                        System.out.print(Smart.avgMemTotal/Smart.count + ",");
                        System.out.print(Smart.maxMemUsed + ",");
                        System.out.println(Smart.avgMemUsed/Smart.count);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                })).start();
                while ((line = reader.readLine()) != null) {
                    if (Main.maxEvents != 0 && events >= Main.maxEvents) {
                        break;
                    }
                    SmartHomesEvent event = SmartHomesEvent.getEventFromString(line);
                    tmp = System.nanoTime();
                    ctx.collect(event);
                    total += System.nanoTime() - tmp;
                    events++;
                    if (events % 10000 == 0) {
                        Smart.avgMemTotal = Runtime.getRuntime().totalMemory();
                        Smart.avgMemTotal += Smart.avgMemTotal;
                        if (Smart.avgMemTotal > Smart.maxMemTotal) {
                            Smart.maxMemTotal = Smart.avgMemTotal;
                        }
                        System.gc();
                        Smart.avgMemUsed = Smart.avgMemTotal - Runtime.getRuntime().freeMemory();
                        Smart.avgMemUsed += Smart.avgMemUsed;
                        if (Smart.avgMemUsed > Smart.maxMemUsed) {
                            Smart.maxMemUsed = Smart.avgMemUsed;
                        }
                        Smart.count++;
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
                Smart.executionTime = total;
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