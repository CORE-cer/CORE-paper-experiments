package edu.puc.flink;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.*;
import java.util.LinkedList;
import java.util.List;

public class FileEventSource extends RichParallelSourceFunction<Event> {
    private final String fileName;
    public static int events;


    FileEventSource(String streamFileName){
        fileName = streamFileName;
    }

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        if (!Main.memoryTest) {
            try {
                int timeout = Main.timeout;
                FileReader file = new FileReader(fileName);
                BufferedReader reader = new BufferedReader(file);
                LinkedList<Event> eventList = new LinkedList<>();
                long start = System.nanoTime();
                String line;

                while ((line = reader.readLine()) != null){
                    Event event = new Event(line.substring(0, 2), Integer.parseInt(line.substring(6, line.length() - 1)));
                    eventList.add(event);
                }

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

    //        System.out.println((double)(System.nanoTime() - t1)/1000000000 + ",");
        } else {
            try {
                int timeout = Main.timeout;
                FileReader file = new FileReader(fileName);
                BufferedReader reader = new BufferedReader(file);
                long start = System.nanoTime();
                String line;

                long total = 0;
                long used;
                while ((line = reader.readLine()) != null){
                    if (Main.maxEvents != 0 && events >= Main.maxEvents) {
                        break;
                    }
                    events++;
                    Event event = new Event(line.substring(0, 2), Integer.parseInt(line.substring(6, line.length() - 1)));
                    ctx.collect(event);
                    if (events % 10000 == 0) {
                        total = Runtime.getRuntime().totalMemory();
                        Main.avgMemTotal += total;
                        if (total > Main.maxMemTotal) {
                            Main.maxMemTotal = total;
                        }
                        System.gc();
                        used = total - Runtime.getRuntime().freeMemory();
                        Main.avgMemUsed += used;
                        if (used > Main.maxMemUsed) {
                            Main.maxMemUsed = used;
                        }
                        Main.count++;
                    }
                    if (timeout != 0 && System.nanoTime() - start >= timeout * 1000000000L) {
                        //                    System.err.println(event.getId());
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

            //        System.out.println((double)(System.nanoTime() - t1)/1000000000 + ",");

        }
    }

    @Override
    public void cancel() { }
}