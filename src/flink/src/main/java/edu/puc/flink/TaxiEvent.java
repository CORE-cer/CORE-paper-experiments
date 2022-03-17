package edu.puc.flink;


import java.util.Arrays;
import java.util.List;

public class TaxiEvent extends Event {

    String type;
    int id;
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

    private TaxiEvent() {
        super();
    }

    @Override
    public String toString(){
        return type + "(id=" + id + ")";
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

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setFare_amount(double fare_amount) {
        this.fare_amount = fare_amount;
    }

    public void setType(String type) {
        this.type = type;
    }

    public long getDropoff_datetime() {
        return dropoff_datetime;
    }

    public String getPickup_zone() {
        return pickup_zone;
    }

    public String getDropoff_zone() {
        return dropoff_zone;
    }

    static public TaxiEvent getEventFromString(String data) {
        TaxiEvent toReturn = new TaxiEvent();
        int offset = 4;
        toReturn.setType(data.substring(0, offset));
        data = data.substring(offset, data.length() - 1);
        List<String> data_values = Arrays.asList(data.split(","));

        toReturn.setId(Integer.parseInt(data_values.get(0).split("=")[1]));
        toReturn.setMedallion(data_values.get(1).split("=")[1]);
        toReturn.setHack_license(data_values.get(2).split("=")[1]);
        toReturn.setPickup_datetime(Long.parseLong(data_values.get(3).split("=")[1]));
        toReturn.setDropoff_datetime(Long.parseLong(data_values.get(4).split("=")[1]));
        toReturn.setTrip_time_in_secs(Integer.parseInt(data_values.get(5).split("=")[1]));
        toReturn.setTrip_distance(Double.parseDouble(data_values.get(6).split("=")[1]));
        toReturn.setPickup_zone(data_values.get(7).split("=")[1]);
        toReturn.setDropoff_zone(data_values.get(8).split("=")[1]);
        toReturn.setPayment_type(data_values.get(9).split("=")[1]);
        toReturn.setFare_amount(Double.parseDouble(data_values.get(10).split("=")[1]));
        toReturn.setSurcharge(Double.parseDouble(data_values.get(11).split("=")[1]));
        toReturn.setMta_tax(Double.parseDouble(data_values.get(12).split("=")[1]));
        toReturn.setTip_amount(Double.parseDouble(data_values.get(13).split("=")[1]));
        toReturn.setTolls_amount(Double.parseDouble(data_values.get(14).split("=")[1]));
        toReturn.setTotal_amount(Double.parseDouble(data_values.get(15).split("=")[1]));

        return toReturn;
    }

}
