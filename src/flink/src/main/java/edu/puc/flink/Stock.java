package edu.puc.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
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

public class Stock {
    private static long totalResults = 0;
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
    public static int limit = 10;

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
                .addSource(new FileStockEventSource(streamFileName))
//                .keyBy((KeySelector<Event, Integer>) value -> ((StockEvent) value).getVolume())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> ((StockEvent) event).getStock_time()));

        PatternStream<Event> patternStream = CEP.pattern(inputStream, query);


        DataStream<String> results = patternStream.process(new PatternProcessFunction<Event, String>() {
            @Override
            public void processMatch(Map<String, List<Event>> match, Context ctx, Collector<String> out) {
                long t0 = System.nanoTime();
                totalResults++;
                count++;
                if (count > limit) {
                    return;
                }
                System.err.print(match.get("SELL") + " ");
                System.err.println(match.get("BUY"));
                enumerationTime += System.nanoTime() - t0;
            }
        });

        results.print();
        long start = System.nanoTime();
        env.execute();

//        System.out.println("Number of matches: " + totalResults);

//        System.out.print(0 + ",");

        if (!memoryTest) {
            System.out.print((double)(System.nanoTime() - start)/1000000000 + ",");
            System.out.print(FileStockEventSource.events + ",");
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
        }
        return null;
    }

    private static AfterMatchSkipStrategy skipStrategy = AfterMatchSkipStrategy.skipPastLastEvent();

    private static Pattern<Event, ?> pattern_q1(Integer timeWindow) {

        Pattern<Event, ?> toReturn = Pattern.<Event>begin("MSFT", skipStrategy)
                .where(Conditions.equals("SELL"))
                .where(Conditions.equals_name("MSFT"))

                .followedByAny("ORCL")
                .where(Conditions.equals("BUY"))
                .where(Conditions.equals_name("ORCL"))

                .followedByAny("CSCO")
                .where(Conditions.equals("BUY"))
                .where(Conditions.equals_name("CSCO"))

                .followedByAny("AMAT")
                .where(Conditions.equals("SELL"))
                .where(Conditions.equals_name("AMAT"));

        return toReturn.within(Time.milliseconds(timeWindow));
    }

    private static Pattern<Event, ?> pattern_q2(Integer timeWindow) {

        Pattern<Event, ?> toReturn = Pattern.<Event>begin("MSFT", skipStrategy)
                .where(Conditions.equals("SELL"))
                .where(Conditions.equals_name("MSFT"))
                .where(Conditions.price_greater(26.0))

                .followedByAny("ORCL")
                .where(Conditions.equals("BUY"))
                .where(Conditions.equals_name("ORCL"))
                .where(Conditions.price_greater(11.14))

                .followedByAny("CSCO")
                .where(Conditions.equals("BUY"))
                .where(Conditions.equals_name("CSCO"))

                .followedByAny("AMAT")
                .where(Conditions.equals("SELL"))
                .where(Conditions.equals_name("AMAT"))
                .where(Conditions.price_greater_equal(18.92));

        return toReturn.within(Time.milliseconds(timeWindow));
    }

    private static Pattern<Event, ?> pattern_q3(Integer timeWindow) {

        Pattern<Event, ?> toReturn = Pattern.<Event>begin("MSFT", skipStrategy)
                .where(Conditions.equals("SELL"))
                .where(Conditions.equals_name("MSFT"))

                .followedByAny("ORCL")
                .where(Conditions.equals("BUY"))
                .where(Conditions.equals_name("ORCL"))

                .followedByAny("CSCO")
                .where(Conditions.equals("BUY"))
                .where(Conditions.equals_name("CSCO"))

                .followedByAny("AMAT")
                .where(Conditions.equals("SELL"))
                .where(Conditions.equals_name("AMAT"));

        return toReturn.within(Time.milliseconds(timeWindow));
    }

    private static Pattern<Event, ?> pattern_q4(Integer timeWindow) {

        Pattern<Event, ?> toReturn = Pattern.<Event>begin("MSFT", skipStrategy)
                .where(Conditions.equals("SELL"))
                .where(Conditions.equals_name("MSFT"))

                .followedByAny("ORCL")
                .where(Conditions.buy_or_sell())
                .where(Conditions.equals_name("ORCL"))

                .followedByAny("CSCO")
                .where(Conditions.buy_or_sell())
                .where(Conditions.equals_name("CSCO"))

                .followedByAny("AMAT")
                .where(Conditions.equals("SELL"))
                .where(Conditions.equals_name("AMAT"));

        return toReturn.within(Time.milliseconds(timeWindow));
    }

    private static Pattern<Event, ?> pattern_q5(Integer timeWindow) {

        Pattern<Event, ?> toReturn = Pattern.<Event>begin("MSFT", skipStrategy)
                .where(Conditions.equals("SELL"))
                .where(Conditions.equals_name("MSFT"))
                .where(Conditions.price_greater(26.0))

                .followedByAny("ORCL")
                .where(Conditions.buy_or_sell())
                .where(Conditions.equals_name("ORCL"))
                .where(Conditions.price_greater(11.14))

                .followedByAny("CSCO")
                .where(Conditions.buy_or_sell())
                .where(Conditions.equals_name("CSCO"))

                .followedByAny("AMAT")
                .where(Conditions.equals("SELL"))
                .where(Conditions.equals_name("AMAT"))
                .where(Conditions.price_greater_equal(18.92));

        return toReturn.within(Time.milliseconds(timeWindow));
    }

    private static Pattern<Event, ?> pattern_q6(Integer timeWindow) {

        Pattern<Event, ?> toReturn = Pattern.<Event>begin("MSFT", skipStrategy)
                .where(Conditions.equals("SELL"))
                .where(Conditions.equals_name("MSFT"))

                .followedByAny("ORCL")
                .where(Conditions.buy_or_sell())
                .where(Conditions.equals_name("ORCL"))

                .followedByAny("CSCO")
                .where(Conditions.buy_or_sell())
                .where(Conditions.equals_name("CSCO"))

                .followedByAny("AMAT")
                .where(Conditions.equals("SELL"))
                .where(Conditions.equals_name("AMAT"));

        return toReturn.within(Time.milliseconds(timeWindow));
    }

    private static Pattern<Event, ?> pattern_q7(Integer timeWindow) {

        Pattern<Event, ?> toReturn = Pattern.<Event>begin("MSFT", skipStrategy)
                .where(Conditions.equals("SELL"))
                .where(Conditions.equals_name("MSFT"))

                .followedByAny("ORCL")
                .oneOrMore()
                .where(Conditions.buy_or_sell())
                .where(Conditions.equals_name("QQQ"))
                .where(Conditions.volume_equals(4000))

                .followedByAny("AMAT")
                .where(Conditions.equals("SELL"))
                .where(Conditions.equals_name("AMAT"));

        return toReturn.within(Time.milliseconds(timeWindow));
    }
}
