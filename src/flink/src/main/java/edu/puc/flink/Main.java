package edu.puc.flink;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import scala.Int;

import java.util.List;
import java.util.Map;

public class Main {
    private static long totalResults = 0;
    public static long enumerationTime = 0;
    public static int timeout = 0;
    private static int events = 0;
    public static boolean memoryTest = false;
    public static int maxEvents = 0;
    public static long avgMemTotal = 0;
    public static long maxMemTotal = 0;
    public static long avgMemUsed = 0;
    public static long maxMemUsed = 0;
    public static int count = 0;

    public static void main(String[] args) throws Exception {
        if (args.length > 4) {
            timeout = Integer.parseInt(args[4]);
        }
        if (args.length > 5) {
            memoryTest = Boolean.parseBoolean(args[5]);
        }
        if (args.length > 6) {
            maxEvents = Integer.parseInt(args[6]);
        }
        cep(args[0], getQuery(args[1], args[2], args[3]));
    }

    private static void cep(String streamFileName, Pattern<Event, ?> query) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Event> inputStream = env
                .addSource(new FileEventSource(streamFileName))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.getId()));

        PatternStream<Event> patternStream = CEP.pattern(inputStream, query);


        DataStream<String> results = patternStream.process(new PatternProcessFunction<Event, String>() {
            @Override
            public void processMatch(Map<String, List<Event>> match, Context ctx, Collector<String> out) {
                long t0 = System.nanoTime();
                totalResults++;
                for (String key : match.keySet()) {
                    System.err.print(match.get(key).get(0) + " ");
                }
                System.err.println();
                enumerationTime += System.nanoTime() - t0;
            }
        });

        results.print();
//        long t0 = System.nanoTime();
        env.execute();
//        System.out.println((double)(System.nanoTime() - t0)/1000000000 + ",");

//        System.out.println("Number of matches: " + totalResults);

        if (!memoryTest) {
            System.out.print(FileEventSource.events + ",");
            System.out.print((double)enumerationTime/1000000000 + ",");
            System.out.print(totalResults);
            System.out.println();
        } else {
            System.out.print(maxMemTotal + ",");
            System.out.print(avgMemTotal/count + ",");
            System.out.print(maxMemUsed + ",");
            System.out.println(avgMemUsed/count);
        }

//        System.out.println("Total execution time: " + (endTime - startTime) / 1000000);
    }


    private static Pattern<Event, ?> getQuery(String queryType, String queryName, String noMatches) {
        Boolean nm = Boolean.parseBoolean(noMatches);
        Pattern<Event, ?> toReturn = null;
        if (queryType.equals("kleene")) {
            toReturn = pattern_custom_kleene(queryName, nm);
        } else if (queryType.equals("seq")) {
            toReturn = pattern_custom(queryName, nm);
        } else if (queryType.equals("disj")) {
            toReturn = pattern_custom_disjunction(queryName, nm);
        }
        return toReturn;
    }

    private static AfterMatchSkipStrategy skipStrategy = AfterMatchSkipStrategy.skipPastLastEvent();

    private static Pattern<Event, ?> pattern_custom(String data, Boolean nm) {
        String letters = "ABCDEFGHIJKLMNOPQRSTYUVWXYZ";
        String[] d = data.split(" ");

        events = Integer.parseInt(d[0]);
        Integer timeWindow = Integer.parseInt(d[1]);


        Pattern<Event, ?> toReturn = Pattern.<Event>begin("AA", skipStrategy).where(Conditions.equals("AA"));

        int addedEvents = 1;

        for (int i = 0; i < letters.length(); i++){
            char c1 = letters.charAt(i);
            for (int j = 0; j < letters.length(); j++){
                char c2 = letters.charAt(j);
                if (c1 == 'A') {
                    if (c2 == 'S' || c2 == 'T' || c2 == 'A') {
                        continue;
                    }
                }

                String event = "" + c1 + c2;

                toReturn = toReturn.followedByAny(event).where(Conditions.equals(event));

                addedEvents++;
                if (events == addedEvents) {
                    if (nm) {
                        toReturn = toReturn.followedByAny("ZZ").where(Conditions.equals("ZZ"));
                    }
                    return toReturn.within(Time.milliseconds(timeWindow));
                }
            }
        }

        return toReturn.within(Time.milliseconds(timeWindow));
    }

    private static Pattern<Event, ?> pattern_custom_kleene(String data, Boolean nm) {
        String letters = "ABCDEFGHIJKLMNOPQRSTYUVWXYZ";
        String[] d = data.split(" ");

        Integer events = Integer.parseInt(d[0]);
        Integer timeWindow = Integer.parseInt(d[1]);


        Pattern<Event, ?> toReturn = Pattern.<Event>begin("AA", skipStrategy).where(Conditions.equals("AA"));

        int addedEvents = 1;

        for (int i = 0; i < letters.length(); i++){
            char c1 = letters.charAt(i);
            for (int j = 0; j < letters.length(); j++){
                char c2 = letters.charAt(j);
                if (c1 == 'A') {
                    if (c2 == 'S' || c2 == 'T' || c2 == 'A') {
                        continue;
                    }
                }

                String event = "" + c1 + c2;

                if (j % 2 == 1) {
                    toReturn = toReturn.followedByAny(event).oneOrMore().where(Conditions.equals(event));
                } else {
                    toReturn = toReturn.followedByAny(event).where(Conditions.equals(event));
                }

                addedEvents++;
                if (events == addedEvents) {
                    if (nm) {
                        toReturn = toReturn.followedByAny("ZZ").where(Conditions.equals("ZZ"));
                    }
                    return toReturn.within(Time.milliseconds(timeWindow));
                }
            }
        }

        return toReturn.within(Time.milliseconds(timeWindow));
    }

    private static Pattern<Event, ?> pattern_custom_disjunction(String data, Boolean nm) {
        String letters = "ABCDEFGHIJKLMNOPQRSTYUVWXYZ";
        String[] d = data.split(" ");

        Integer events = Integer.parseInt(d[0]);
        Integer timeWindow = Integer.parseInt(d[1]);


        Pattern<Event, ?> toReturn = Pattern.<Event>begin("AA", skipStrategy).where(Conditions.equals("AA"));

        int addedEvents = 1;
        boolean disj = true;

        for (int i = 0; i < letters.length(); i++){
            char c1 = letters.charAt(i);
            int j = -1;
            while (j < letters.length()) {
                j++;
                char c2 = letters.charAt(j);
                if (c1 == 'A') {
                    if (c2 == 'S' || c2 == 'T' || c2 == 'A') {
                        continue;
                    }
                }

                String event = "" + c1 + c2;

                if (disj) {
                    int k = j + 1;
                    char c3;
                    while (true) {
                        c3 = letters.charAt(k);
                        if (c1 == 'A') {
                            if (c3 == 'S' || c3 == 'T' || c3 == 'A') {
                                continue;
                            }
                        }
                        break;
                    }

                    String event2 = "" + c1 + c3;

                    toReturn = toReturn.followedByAny(event + " OR " + event2).oneOrMore().or(Conditions.or(event, event2));
                    j = k;
                    addedEvents++;
                    disj = false;
                } else {
                    toReturn = toReturn.followedByAny(event).where(Conditions.equals(event));
                    disj = true;
                }

                addedEvents++;
                if (events <= addedEvents) {
                    if (nm) {
                        toReturn = toReturn.followedByAny("ZZ").where(Conditions.equals("ZZ"));
                    }
                    return toReturn.within(Time.milliseconds(timeWindow));
                }
            }
        }

        return toReturn.within(Time.milliseconds(timeWindow));
    }
}
