package com.cepl.espertest;

import java.util.Arrays;
import java.util.List;

public class SmartHomesEvent {

    String eventType;
    int id;
    int plug_timestamp;
    double value;

    int plug_id;
    int household_id;



    private SmartHomesEvent() {}

    @Override
    public String toString(){
        return eventType + "(id=" + id + ")";
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public void setPlug_id(int plug_id) {
        this.plug_id = plug_id;
    }

    public void setHousehold_id(int household_id) {
        this.household_id = household_id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setPlug_timestamp(int plug_timestamp) {
        this.plug_timestamp = plug_timestamp;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public double getValue() {
        return value;
    }

    public int getHousehold_id() {
        return household_id;
    }

    public int getId() {
        return id;
    }

    public int getPlug_id() {
        return plug_id;
    }

    public int getPlug_timestamp() {
        return plug_timestamp;
    }

    public String getEventType() {
        return eventType;
    }

    static public SmartHomesEvent getEventFromString(String data) {
        SmartHomesEvent toReturn = new SmartHomesEvent();
        int offset = 4;
        toReturn.setEventType(data.substring(0, offset));
        data = data.split("[()]")[1];
        List<String> data_values = Arrays.asList(data.split(","));
        toReturn.setId(Integer.parseInt(data_values.get(0).split("=")[1]));
        toReturn.setPlug_timestamp(Integer.parseInt(data_values.get(1).split("=")[1]));
        toReturn.setValue(Double.parseDouble(data_values.get(2).split("=")[1]));
        toReturn.setPlug_id(Integer.parseInt(data_values.get(3).split("=")[1]));
        toReturn.setHousehold_id(Integer.parseInt(data_values.get(4).split("=")[1]));

        return toReturn;
    }

}
