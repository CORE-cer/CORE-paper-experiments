package edu.puc.flink;

import org.apache.flink.cep.pattern.conditions.SimpleCondition;

import java.util.Objects;

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

    public static SimpleCondition<Event> value_grater(int val) {
        return new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event value) {
                if (value instanceof SmartHomesEvent) {
                    return ((SmartHomesEvent) value).getValue() > val;
                }
                return false;
            }
        };
    }

    public static SimpleCondition<Event> hh_id_equals(int val) {
        return new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event value) {
                if (value instanceof SmartHomesEvent) {
                    return ((SmartHomesEvent) value).getHousehold_id() == val;
                }
                return false;
            }
        };
    }

    public static SimpleCondition<Event> pickup_zone_equals(String val) {
        return new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event value) {
                if (value instanceof TaxiEvent) {
                    return Objects.equals(((TaxiEvent) value).getPickup_zone(), val);
                }
                return false;
            }
        };
    }

    public static SimpleCondition<Event> dropoff_zone_equals(String val) {
        return new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event value) {
                if (value instanceof TaxiEvent) {
                    return Objects.equals(((TaxiEvent) value).getDropoff_zone(), val);
                }
                return false;
            }
        };
    }
}
