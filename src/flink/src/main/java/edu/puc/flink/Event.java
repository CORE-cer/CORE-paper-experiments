package edu.puc.flink;

public class Event{
    private String type;
    private int id;

    String getType(){
        return type;
    }

    int getId(){
        return id;
    }

    Event(String type, int id){
        this.type = type;
        this.id = id;
    }

    protected Event() {};

    @Override
    public String toString() {
        return type + "(id=" + id + ")";
    }
}