/*
 * Copyright (c) 2011, Regents of the University of Massachusetts Amherst
 * All rights reserved.

 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met:

 *   * Redistributions of source code must retain the above copyright notice, this list of conditions
 * 		and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright notice, this list of conditions
 * 		and the following disclaimer in the documentation and/or other materials provided with the distribution.
 *   * Neither the name of the University of Massachusetts Amherst nor the names of its contributors
 * 		may be used to endorse or promote products derived from this software without specific prior written
 * 		permission.

 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
 * FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package edu.umass.cs.sase.stream;


import java.util.*;

/**
 * This class represents a kind of event.
 * @author haopeng
 *
 */
public class SmartHomeEvent implements Event {

    private static int nameCounter = 0;
    private static int counter = 1;

    int id;

    String eventType;
    int my_id;
    int plug_timestamp;
    double value;

    int plug_id;
    int household_id;

    /**
     * Constructor
     */
//    public SmartHomeEvent(int i, int ts, String et, String n, int vol, double p, int st){
//        id = i;
//        timestamp = ts;
//        eventType = et;
//        price = p;
//        name = n;
//        volume = vol;
//        stock_time = st;
//    }
//
//    public SmartHomeEvent(int id, String et){
//        this.id = counter++;
//        my_id = id;
//        // timestamp = ts;
//        eventType = et;
//        // price = p;
//
//    }

    private SmartHomeEvent() {}

    /**
     * @return the cloned event
     */
    public Object clone(){
        SmartHomeEvent o = null;
        try {
            o = (SmartHomeEvent)super.clone();
        } catch (CloneNotSupportedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return o;
    }

    /**
     * @return the id
     */
    public int getId() {
        return id;
    }

    /**
     * @param id the id to set
     */
    public void setId(int id) {
        this.id = id;
    }

    /**
     * @return the timestamp
     */
    public int getTimestamp() {
        return plug_timestamp;
    }

    /**
     * @param timestamp the timestamp to set
     */
    public void setTimestamp(int timestamp) {
        this.plug_timestamp = timestamp;
    }

    /**
     * @return the eventType
     */
    public String getEventType() {
        return eventType;
    }

    /**
     * @param eventType the eventType to set
     */
    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    /**
     * @return the price
     */
    public double getValue() {
        return value;
    }

    /**
     * @param price the price to set
     */
    public void setValue(double price) {
        this.value = price;
    }

    public String toString(){
        // return "ID="+ id + "\tTimestamp=" + timestamp
        // 	+ "\tEventType=" + eventType + "\tPrice =" + price;
        return eventType + "(id=" + my_id + ")";
    }

    /* (non-Javadoc)
     * @see edu.umass.cs.sase.mvc.model.Event#getAttributeByName(java.lang.String)
     */
    public int getAttributeByName(String attributeName) {

        if(attributeName.equalsIgnoreCase("id"))
            return this.id;
        if(attributeName.equalsIgnoreCase("plug_timestamp"))
            return this.plug_timestamp;
        if(attributeName.equalsIgnoreCase("value"))
            return (int) (this.value*1000);
        if(attributeName.equalsIgnoreCase("plug_id"))
            return this.plug_id;
        if(attributeName.equalsIgnoreCase("household_id"))
            return this.household_id;
        return 0;
    }

    /* (non-Javadoc)
     * @see edu.umass.cs.sase.mvc.model.Event#getAttributeByNameString(java.lang.String)
     */
    public String getAttributeByNameString(String attributeName) {
        // TODO Auto-generated method stub
        return null;
    }
    /* (non-Javadoc)
     * @see edu.umass.cs.sase.mvc.model.Event#getAttributeValueType(java.lang.String)
     */
    public int getAttributeValueType(String attributeName) {
        // TODO Auto-generated method stub
        return 0;
    }

    /* (non-Javadoc)
     * @see edu.umass.cs.sase.mvc.model.Event#getAttributeByNameDouble(java.lang.String)
     */
    public double getAttributeByNameDouble(String attributeName) {
        if(attributeName.equalsIgnoreCase("value"))
            return value;
        return -1;
    }



    public int getMy_id() {
        return my_id;
    }

    public void setMy_id(int my_id) {
        this.my_id = my_id;
    }

    public void setHousehold_id(int household_id) {
        this.household_id = household_id;
    }

    public void setPlug_id(int plug_id) {
        this.plug_id = plug_id;
    }

    static public SmartHomeEvent getEventFromString(String data) {
        SmartHomeEvent toReturn = new SmartHomeEvent();
        int offset = 4;
        toReturn.setEventType(data.substring(0, offset));
        data = data.split("[()]")[1];
        List<String> data_values = Arrays.asList(data.split(","));
        toReturn.setMy_id(Integer.parseInt(data_values.get(0).split("=")[1]));
        toReturn.setTimestamp(Integer.parseInt(data_values.get(1).split("=")[1]));
        toReturn.setValue(Double.parseDouble(data_values.get(2).split("=")[1]));
        toReturn.setPlug_id(Integer.parseInt(data_values.get(3).split("=")[1]));
        toReturn.setHousehold_id(Integer.parseInt(data_values.get(4).split("=")[1]));

        toReturn.setId(counter++);
        return toReturn;
    }
}
