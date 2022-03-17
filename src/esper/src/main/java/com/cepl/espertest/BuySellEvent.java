package com.cepl.espertest;

import java.util.Arrays;
import java.util.List;

public class BuySellEvent {
    private String type;
    private String name;
    private int id;
    private int volume;
    private double price;
    private int stock_time;
    private int timestamp;

    public BuySellEvent(String type, String name, int id, int volume, double price, int stock_time){
        this.type = type;
        this.id = id;
        this.name = name;
        this.volume = volume;
        this.price = price;
        this.stock_time = stock_time;
        this.timestamp = stock_time;
    }

    private BuySellEvent() {}

	public int getId() {
		return id;
    }
    
    public String getType() {
		return type;
    }

    public String getName() {
        return name;
    }

    public double getPrice() {
        return price;
    }

    public int getStock_time() {
        return stock_time;
    }

    public int getVolume() {
        return volume;
    }

    public int getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(int timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString(){
        return type + "(id=" + id + " name=" + name + ")";
    }

    public void setStock_time(int stock_time) {
        this.stock_time = stock_time;
    }

    public void setVolume(int volume) {
        this.volume = volume;
    }


    public void setName(String name) {
        this.name = name;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public void setType(String type) {
        this.type = type;
    }

    static public BuySellEvent getEventFromString(String data) {
        BuySellEvent toReturn = new BuySellEvent();
        int offset = 0;
        if (data.startsWith("B")){
            offset += 3;
        } else {
            offset += 4;
        }
        toReturn.setType(data.substring(0, offset));
        data = data.split("[()]")[1];
        List<String> data_values = Arrays.asList(data.split(","));
        toReturn.setId(Integer.parseInt(data_values.get(0).split("=")[1]));
        toReturn.setName(data_values.get(1).split("=")[1]);
        toReturn.setVolume(Integer.parseInt(data_values.get(2).split("=")[1]));
        toReturn.setPrice(Double.parseDouble(data_values.get(3).split("=")[1]));
        toReturn.setStock_time(Integer.parseInt(data_values.get(4).split("=")[1]));
        toReturn.setTimestamp(toReturn.getStock_time());

        return toReturn;
    }

}
