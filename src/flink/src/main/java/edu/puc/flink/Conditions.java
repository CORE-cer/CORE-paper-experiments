package edu.puc.flink;

import org.apache.flink.cep.pattern.conditions.SimpleCondition;

public class Conditions {

    public static SimpleCondition<Event> equals(String eventType){
        return new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event value) {
                return eventType.equals(value.getType());
            }
        };
    }

    public static SimpleCondition<Event> or(String eventType1, String eventType2){
        return new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event value) {
                return eventType1.equals(value.getType()) || eventType2.equals(value.getType());
            }
        };
    }

    public static SimpleCondition<Event> equals_name(String name){
        return new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event value) {
                if (value instanceof StockEvent) {
                    return name.equals(((StockEvent) value).getName());
                }
                return false;
            }
        };
    }

    public static SimpleCondition<Event> price_greater(Double price){
        return new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event value) {
                if (value instanceof StockEvent) {
                    return price < ((StockEvent) value).getPrice();
                }
                return false;
            }
        };
    }

    public static SimpleCondition<Event> price_greater_equal(Double price){
        return new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event value) {
                if (value instanceof StockEvent) {
                    return price <= ((StockEvent) value).getPrice();
                }
                return false;
            }
        };
    }

    public static SimpleCondition<Event> buy_or_sell(){
        return new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event value) {
                if (value instanceof StockEvent) {
                    return ((StockEvent) value).getType().equals("BUY") || ((StockEvent) value).getType().equals("SELL");
                }
                return false;
            }
        };
    }

    public static SimpleCondition<Event> volume_equals(int volume){
        return new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event value) {
                if (value instanceof StockEvent) {
                    return ((StockEvent) value).getVolume() == volume;
                }
                return false;
            }
        };
    }
}
