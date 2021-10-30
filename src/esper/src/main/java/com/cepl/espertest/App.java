package com.cepl.espertest;

import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.runtime.client.*;
import com.espertech.esper.common.client.*;
import com.espertech.esper.compiler.client.*;

import java.io.*;
import java.util.*;

public class App 
{
    public static void main(String[] args) throws FileNotFoundException, EPCompileException, EPDeployException {
        long maxMemTotal = 0;
        long avgMemTotal = 0;
        long maxMemUsed = 0;
        long avgMemUsed = 0;
        int count = 0;
        long totalMem = Runtime.getRuntime().totalMemory();
//        maxMemTotal = avgMemTotal = totalMem;
        System.gc();
        long usedMem = totalMem - Runtime.getRuntime().freeMemory();
//        maxMemUsed = avgMemUsed = usedMem;

        String stream_file = args.length > 1? args[1] : "../../streams/ABCD_1_zipf.txt";
        boolean memoryTest = Boolean.parseBoolean(args[2]);
        boolean double_letter = Boolean.parseBoolean(args[3]);
        int max_events = args.length > 4? Integer.parseInt(args[4]) : 0;
        int timeout = args.length > 5? Integer.parseInt(args[5]) : 0;



        String entireFileText = new Scanner(new File(args[0]))
                .useDelimiter("\\A").next();

        long startCompileTime = System.nanoTime();

        Configuration configuration = new Configuration();
        configuration.getCommon().addEventType(BuySellEvent.class);

        EPRuntime engine = EPRuntimeProvider.getDefaultRuntime(configuration);
        CompilerArguments compilerArguments = new CompilerArguments(configuration);
        compilerArguments.getPath().add(engine.getRuntimePath());

        EPCompiled compiled = EPCompilerProvider.getCompiler().compile(entireFileText, compilerArguments);

        EPDeployment statement = engine.getDeploymentService().deploy(compiled);
        long compileTime = System.nanoTime() - startCompileTime;

        statement.getStatements()[0].addListener(new EventListener());
        
        FileReader file;
        BufferedReader stream;

        if (!memoryTest) {
            try {
                file = new FileReader(stream_file);
                stream = new BufferedReader(file);

                String line;
                int events = 0;
                BuySellEvent ev;
//                MyEvent ev;
                LinkedList<BuySellEvent> eventsList = new LinkedList<>();
                long t0 = System.nanoTime();
                while (max_events == 0 || events <= max_events) {
                    line = stream.readLine();
                    if (line == null) {
                        break;
                    }
                    events++;
                    eventsList.add(BuySellEvent.getEventFromString(line));
                }

                long start = System.nanoTime();
                int events2 = 0;

                while (!eventsList.isEmpty()) {
                    ev = eventsList.removeFirst();

                    engine.getEventService().sendEventBean(ev, "BuySellEvent");
                    long time = System.nanoTime();
                    if (timeout != 0 && time - start > timeout * 1000000000L) {
                        break;
                    }
                    events2++;
                }

                System.out.print((double)(System.nanoTime() - start)/1000000000 + ",");
                System.out.print(events2 + ",");
                //                System.out.print((((double)parsingTime) / 1000000000) + ",");
                //                System.out.print((((double)compileTime) / 1000000000) + ",");
                //                System.out.print((((double)total) / 1000000000) + ",");
                System.out.print((((double) EventListener.totalTime) / 1000000000L) + ",");
                System.out.println(EventListener.totalMatches);


                stream.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        } else {
            try {
                file = new FileReader(stream_file);
                stream = new BufferedReader(file);

                String line;
                int events = 0;
                long start = System.nanoTime();

                while (max_events == 0 || events <= max_events) {
                    line = stream.readLine();
                    if (line == null) {
                        break;
                    }
                    events++;
                    engine.getEventService().sendEventBean(BuySellEvent.getEventFromString(line), "BuySellEvent");

                    if (events % 10000 == 0) {
                        totalMem = Runtime.getRuntime().totalMemory();
                        avgMemTotal += totalMem;
                        if (totalMem > maxMemTotal) {
                            maxMemTotal = totalMem;
                        }
                        System.gc();
                        usedMem = totalMem - Runtime.getRuntime().freeMemory();
                        avgMemUsed += usedMem;
                        if (usedMem > maxMemUsed) {
                            maxMemUsed = usedMem;
                        }
                        count++;
                    }

                    if (timeout != 0 && System.nanoTime() - start > timeout * 1000000000L) {
                        break;
                    }
                }

                System.out.print(maxMemTotal + ",");
                System.out.print(avgMemTotal / count + ",");
                System.out.print(maxMemUsed + ",");
                System.out.println(avgMemUsed / count);

                stream.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    private static MyEvent parseEvent(String line, boolean double_letter) {
        if (double_letter) {
            return new MyEvent(line.substring(0, 2), Integer.parseInt(line.substring(6, line.length() - 1)));
        } else {
            return new MyEvent(line.substring(0, 1), Integer.parseInt(line.substring(5, line.length() - 1)));
        }
        //                ev = BuySellEvent.getEventFromString(line);
    }
}
