package edu.puc.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

public class Smart {
    public static long totalResults = 0;
    public static long enumerationTime = 0;
    public static long executionTime = 0;
    public static int timeout = 0;
    private static int events = 0;
    public static boolean memoryTest = false;
    public static int maxEvents = 0;
    public static long avgMemTotal = 0;
    public static long maxMemTotal = 0;
    public static long avgMemUsed = 0;
    public static long maxMemUsed = 0;
    public static int count = 0;
    public static int limit = 1000;

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
        cep(args[0], getQuery(args[1], Integer.parseInt(args[2])));
    }

    private static void cep(String streamFileName, Pattern<Event, ?> query) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Event> inputStream = env
                .addSource(new FileSmartHomesEventSource(streamFileName))
//                .keyBy((KeySelector<Event, Integer>) value -> ((StockEvent) value).getVolume())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> ((SmartHomesEvent) event).getPlug_timestamp()));

        PatternStream<Event> patternStream = CEP.pattern(inputStream, query);


        DataStream<String> results = patternStream.process(new PatternProcessFunction<Event, String>() {
            @Override
            public void processMatch(Map<String, List<Event>> match, Context ctx, Collector<String> out) {
                long t0 = System.nanoTime();
                totalResults++;
                count++;
                if (count > limit) {
                    count = 0;
                    return;
                }
                System.err.print(match.get("H1") + " ");
                System.err.print(match.get("H2") + " ");
                System.err.println(match.get("H3"));
                enumerationTime += System.nanoTime() - t0;
            }
        });

        results.print();
        long start = System.nanoTime();
        env.execute();

//        System.out.println("Number of matches: " + totalResults);

//        System.out.print(0 + ",");

        if (!memoryTest) {
//            System.out.print((double)(System.nanoTime() - start)/1000000000 + ",");
//            System.out.print(FileSmartHomesEventSource.events + ",");
//            System.out.print((double)enumerationTime/1000000000 + ",");
//            System.out.print(totalResults);
//            System.out.println();
        } else {
            if (count == 0) {
                count = 1;
            }
            System.out.print(maxMemTotal + ",");
            System.out.print(avgMemTotal/count + ",");
            System.out.print(maxMemUsed + ",");
            System.out.println(avgMemUsed/count);
        }

//        System.out.println("Total execution time: " + (endTime - startTime) / 1000000);
    }


    private static Pattern<Event, ?> getQuery(String queryNum, Integer timeWindow) {
        switch (queryNum) {
            case "1":
                return pattern_q1(timeWindow);
            case "2":
                return pattern_q2(timeWindow);
            case "3":
                return pattern_q3(timeWindow);
            case "4":
                return pattern_q4(timeWindow);
            case "5":
                return pattern_q5(timeWindow);
            case "6":
                return pattern_q6(timeWindow);
            case "7":
                return pattern_q7(timeWindow);
            case "8":
                return pattern_q8(timeWindow);
            case "9":
                return pattern_q9(timeWindow);
            case "10":
                return pattern_q10(timeWindow);
            case "11":
                return pattern_q11(timeWindow);
        }
        System.out.println(queryNum);
        return null;
    }

    private static AfterMatchSkipStrategy skipStrategy = AfterMatchSkipStrategy.skipPastLastEvent();

    private static Pattern<Event, ?> pattern_q1(Integer timeWindow) {

        Pattern<Event, ?> toReturn = Pattern.<Event>begin("H1", skipStrategy)
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(0))

                .followedByAny("H2")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(2))

                .followedByAny("H3")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(4));

        return toReturn.within(Time.milliseconds(timeWindow));
    }

    private static Pattern<Event, ?> pattern_q2(Integer timeWindow) {

        Pattern<Event, ?> toReturn = Pattern.<Event>begin("H1", skipStrategy)
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(0))

                .followedByAny("H2")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(2))

                .followedByAny("H3")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(4))

                .followedByAny("H4")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(6))

                .followedByAny("H5")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(9))

                .followedByAny("H6")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(10));

        return toReturn.within(Time.milliseconds(timeWindow));
    }

    private static Pattern<Event, ?> pattern_q3(Integer timeWindow) {

        Pattern<Event, ?> toReturn = Pattern.<Event>begin("H1", skipStrategy)
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(0))

                .followedByAny("H2")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(2))

                .followedByAny("H3")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(4))

                .followedByAny("H4")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(6))

                .followedByAny("H5")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(9))

                .followedByAny("H6")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(10))

                .followedByAny("H7")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(12))

                .followedByAny("H8")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(14))

                .followedByAny("H9")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(15));

        return toReturn.within(Time.milliseconds(timeWindow));
    }

    private static Pattern<Event, ?> pattern_q4(Integer timeWindow) {

        Pattern<Event, ?> toReturn = Pattern.<Event>begin("H1", skipStrategy)
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(0))

                .followedByAny("H2")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(2))

                .followedByAny("H3")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(4))

                .followedByAny("H4")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(6))

                .followedByAny("H5")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(9))

                .followedByAny("H6")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(10))

                .followedByAny("H7")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(12))

                .followedByAny("H8")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(14))

                .followedByAny("H9")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(15))

                .followedByAny("H10")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(4))

                .followedByAny("H11")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(14))

                .followedByAny("H12")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(15));

        return toReturn.within(Time.milliseconds(timeWindow));
    }

    private static Pattern<Event, ?> pattern_q5(Integer timeWindow) {

        Pattern<Event, ?> toReturn = Pattern.<Event>begin("H1", skipStrategy)
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(0))

                .followedByAny("H2")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(2))

                .followedByAny("H3")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(4))

                .followedByAny("H4")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(6))

                .followedByAny("H5")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(9))

                .followedByAny("H6")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(10))

                .followedByAny("H7")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(12))

                .followedByAny("H8")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(14))

                .followedByAny("H9")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(15))

                .followedByAny("H10")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(4))

                .followedByAny("H11")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(14))

                .followedByAny("H12")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(15))

                .followedByAny("H13")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(0))

                .followedByAny("H14")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(2))

                .followedByAny("H15")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(4))

                .followedByAny("H16")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(6))

                .followedByAny("H17")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(9))

                .followedByAny("H18")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(10))

                .followedByAny("H19")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(12))

                .followedByAny("H20")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(14))

                .followedByAny("H21")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(15))

                .followedByAny("H22")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(4))

                .followedByAny("H23")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(14))

                .followedByAny("H24")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(15));

        return toReturn.within(Time.milliseconds(timeWindow));
    }

    private static Pattern<Event, ?> pattern_q6(Integer timeWindow) {

        Pattern<Event, ?> toReturn = Pattern.<Event>begin("H1", skipStrategy)
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(0))

                .followedByAny("H2")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(2))

                .followedByAny("H3")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(4))

                .followedByAny("NE")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(1000));

        return toReturn.within(Time.milliseconds(timeWindow));
    }

    private static Pattern<Event, ?> pattern_q7(Integer timeWindow) {

        Pattern<Event, ?> toReturn = Pattern.<Event>begin("H1", skipStrategy)
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(0))

                .followedByAny("H2")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(2))

                .followedByAny("H3")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(4))

                .followedByAny("H4")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(6))

                .followedByAny("H5")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(9))

                .followedByAny("H6")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(10))

                .followedByAny("NE")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(1000));

        return toReturn.within(Time.milliseconds(timeWindow));
    }

    private static Pattern<Event, ?> pattern_q8(Integer timeWindow) {

        Pattern<Event, ?> toReturn = Pattern.<Event>begin("H1", skipStrategy)
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(0))

                .followedByAny("H2")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(2))

                .followedByAny("H3")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(4))

                .followedByAny("H4")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(6))

                .followedByAny("H5")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(9))

                .followedByAny("H6")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(10))

                .followedByAny("H7")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(12))

                .followedByAny("H8")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(14))

                .followedByAny("H9")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(15))

                .followedByAny("NE")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(1000));

        return toReturn.within(Time.milliseconds(timeWindow));
    }

    private static Pattern<Event, ?> pattern_q9(Integer timeWindow) {

        Pattern<Event, ?> toReturn = Pattern.<Event>begin("H1", skipStrategy)
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(0))

                .followedByAny("H2")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(2))

                .followedByAny("H3")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(4))

                .followedByAny("H4")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(6))

                .followedByAny("H5")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(9))

                .followedByAny("H6")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(10))

                .followedByAny("H7")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(12))

                .followedByAny("H8")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(14))

                .followedByAny("H9")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(15))

                .followedByAny("H10")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(4))

                .followedByAny("H11")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(14))

                .followedByAny("H12")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(15))

                .followedByAny("NE")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(1000));

        return toReturn.within(Time.milliseconds(timeWindow));
    }

    private static Pattern<Event, ?> pattern_q10(Integer timeWindow) {

        Pattern<Event, ?> toReturn = Pattern.<Event>begin("H1", skipStrategy)
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(0))

                .followedByAny("H2")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(2))

                .followedByAny("H3")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(4))

                .followedByAny("H4")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(6))

                .followedByAny("H5")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(9))

                .followedByAny("H6")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(10))

                .followedByAny("H7")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(12))

                .followedByAny("H8")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(14))

                .followedByAny("H9")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(15))

                .followedByAny("H10")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(4))

                .followedByAny("H11")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(14))

                .followedByAny("H12")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(15))

                .followedByAny("H13")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(0))

                .followedByAny("H14")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(2))

                .followedByAny("H15")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(4))

                .followedByAny("H16")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(6))

                .followedByAny("H17")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(9))

                .followedByAny("H18")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(10))

                .followedByAny("H19")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(12))

                .followedByAny("H20")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(14))

                .followedByAny("H21")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(15))

                .followedByAny("H22")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(4))

                .followedByAny("H23")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(14))

                .followedByAny("H24")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(15))

                .followedByAny("NE")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(1000));

        return toReturn.within(Time.milliseconds(timeWindow));
    }

    private static Pattern<Event, ?> pattern_q11(Integer timeWindow) {

        Pattern<Event, ?> toReturn = Pattern.<Event>begin("H1", skipStrategy)
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(0))

                .followedByAny("H2")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(2))

                .followedByAny("H3")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(4))

                .followedByAny("H4")
                .where(Conditions.value_grater(76))
                .where(Conditions.hh_id_equals(1000));

        return toReturn.within(Time.milliseconds(timeWindow));
    }
}
