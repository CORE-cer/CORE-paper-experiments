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


import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class represents a kind of event.
 * @author haopeng
 *
 */
public class TaxiEvent implements Event {


    private static Map<String, Integer> nameMap = new HashMap<>();
    private static int nameCounter = 0;
    private static int counter = 1;

    int id;

    String eventType;
    int my_id;
    String medallion;
    String hack_license;

    long pickup_datetime;
    long dropoff_datetime;

    int trip_time_in_secs;
    double trip_distance;

    String pickup_zone;
    String dropoff_zone;

    String payment_type;
    double fare_amount;
    double surcharge;
    double mta_tax;
    double tip_amount;
    double tolls_amount;
    double total_amount;



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

    private TaxiEvent() {}

    /**
     * @return the cloned event
     */
    public Object clone(){
        TaxiEvent o = null;
        try {
            o = (TaxiEvent)super.clone();
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
        return (int) dropoff_datetime/10;
    }

    /**
     * @param timestamp the timestamp to set
     */
    public void setTimestamp(int timestamp) {
        this.pickup_datetime = timestamp;
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
        if(attributeName.equalsIgnoreCase("pickup_datetime"))
            return (int) pickup_datetime/10;
        if(attributeName.equalsIgnoreCase("dropoff_datetime"))
            return (int) dropoff_datetime/10;
        if(attributeName.equalsIgnoreCase("trip_time_in_secs"))
            return this.trip_time_in_secs;
        if(attributeName.equalsIgnoreCase("pickup_zone"))
            return nameMap.get(pickup_zone);
        if(attributeName.equalsIgnoreCase("dropoff_zone"))
            return nameMap.get(dropoff_zone);

        if (nameMap.get(attributeName) == null) {
            nameMap.put(attributeName, nameCounter);
            nameCounter++;
        }
        return nameMap.get(attributeName);
    }

    /* (non-Javadoc)
     * @see edu.umass.cs.sase.mvc.model.Event#getAttributeByNameString(java.lang.String)
     */
    public String getAttributeByNameString(String attributeName) {
        if(attributeName.equalsIgnoreCase("medallion"))
            return this.medallion;
        if(attributeName.equalsIgnoreCase("hack_license"))
            return this.hack_license;
        if(attributeName.equalsIgnoreCase("pickup_zone"))
            return this.pickup_zone;
        if(attributeName.equalsIgnoreCase("dropoff_zone"))
            return this.dropoff_zone;
        if(attributeName.equalsIgnoreCase("payment_type"))
            return this.payment_type;
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
        if(attributeName.equalsIgnoreCase("trip_distance"))
            return trip_distance;
        if(attributeName.equalsIgnoreCase("fare_amount"))
            return fare_amount;
        if(attributeName.equalsIgnoreCase("surcharge"))
            return surcharge;
        if(attributeName.equalsIgnoreCase("mta_tax"))
            return mta_tax;
        if(attributeName.equalsIgnoreCase("tip_amount"))
            return tip_amount;
        if(attributeName.equalsIgnoreCase("tolls_amount"))
            return tolls_amount;
        if(attributeName.equalsIgnoreCase("total_amount"))
            return total_amount;
        return -1;
    }

    public void setDropoff_datetime(long dropoff_datetime) {
        this.dropoff_datetime = dropoff_datetime;
    }

    public void setDropoff_zone(String dropoff_zone) {
        this.dropoff_zone = dropoff_zone;
    }

    public void setHack_license(String hack_license) {
        this.hack_license = hack_license;
    }

    public void setMedallion(String medallion) {
        this.medallion = medallion;
    }

    public void setMta_tax(double mta_tax) {
        this.mta_tax = mta_tax;
    }

    public void setPayment_type(String payment_type) {
        this.payment_type = payment_type;
    }

    public void setPickup_datetime(long pickup_datetime) {
        this.pickup_datetime = pickup_datetime;
    }

    public void setPickup_zone(String pickup_zone) {
        this.pickup_zone = pickup_zone;
    }

    public void setSurcharge(double surcharge) {
        this.surcharge = surcharge;
    }

    public void setTip_amount(double tip_amount) {
        this.tip_amount = tip_amount;
    }

    public void setTolls_amount(double tolls_amount) {
        this.tolls_amount = tolls_amount;
    }

    public void setTotal_amount(double total_amount) {
        this.total_amount = total_amount;
    }

    public void setTrip_distance(double trip_distance) {
        this.trip_distance = trip_distance;
    }

    public void setTrip_time_in_secs(int trip_time_in_secs) {
        this.trip_time_in_secs = trip_time_in_secs;
    }

    public int getMy_id() {
        return my_id;
    }

    public void setMy_id(int my_id) {
        this.my_id = my_id;
    }

    public void setFare_amount(double fare_amount) {
        this.fare_amount = fare_amount;
    }

    static public TaxiEvent getEventFromString(String data) {
        TaxiEvent toReturn = new TaxiEvent();
        int offset = 4;
        toReturn.setEventType(data.substring(0, offset));
        data = data.substring(offset, data.length() - 1);
        List<String> data_values = Arrays.asList(data.split(","));

        toReturn.setMy_id(Integer.parseInt(data_values.get(0).split("=")[1]));
        toReturn.setMedallion(data_values.get(1).split("=")[1]);
        toReturn.setHack_license(data_values.get(2).split("=")[1]);
        toReturn.setPickup_datetime(Long.parseLong(data_values.get(3).split("=")[1]));
        toReturn.setDropoff_datetime(Long.parseLong(data_values.get(4).split("=")[1]));
        toReturn.setTrip_time_in_secs(Integer.parseInt(data_values.get(5).split("=")[1]));
        toReturn.setTrip_distance(Double.parseDouble(data_values.get(6).split("=")[1]));

        String name = data_values.get(7).split("=")[1];
        if (nameMap.get(name) == null) {
            nameMap.put(name, nameCounter);
            nameCounter++;
        }
        toReturn.setPickup_zone(name);

        name = data_values.get(8).split("=")[1];
        if (nameMap.get(name) == null) {
            nameMap.put(name, nameCounter);
            nameCounter++;
        }
        toReturn.setDropoff_zone(name);
        toReturn.setPayment_type(data_values.get(9).split("=")[1]);
        toReturn.setFare_amount(Double.parseDouble(data_values.get(10).split("=")[1]));
        toReturn.setSurcharge(Double.parseDouble(data_values.get(11).split("=")[1]));
        toReturn.setMta_tax(Double.parseDouble(data_values.get(12).split("=")[1]));
        toReturn.setTip_amount(Double.parseDouble(data_values.get(13).split("=")[1]));
        toReturn.setTolls_amount(Double.parseDouble(data_values.get(14).split("=")[1]));
        toReturn.setTotal_amount(Double.parseDouble(data_values.get(15).split("=")[1]));

        toReturn.setId(counter++);
        return toReturn;
    }
}
