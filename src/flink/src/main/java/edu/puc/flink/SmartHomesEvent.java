package edu.puc.flink;


import java.util.Arrays;
import java.util.List;

public class SmartHomesEvent extends Event {

    String type;
    int id;
    int plug_timestamp;
    double value;

    int plug_id;
    int household_id;


//    public SmartHomesEvent(String type, String name, int id, int volume, double price, int stock_time){
//        super(type, id);
//        this.type = type;
//        this.id = id;
//        this.name = name;
//        this.volume = volume;
//        this.price = price;
//        this.stock_time = stock_time;
//        this.timestamp = stock_time;
//    }

    private SmartHomesEvent() {
        super();
    }


    @Override
    public String toString(){
        return type + "(id=" + id + ")";
    }

    public void setType(String type) {
        this.type = type;
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

    public String getType() {
        return type;
    }

    static public SmartHomesEvent getEventFromString(String data) {
        SmartHomesEvent toReturn = new SmartHomesEvent();
        int offset = 4;
        toReturn.setType(data.substring(0, offset));
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
