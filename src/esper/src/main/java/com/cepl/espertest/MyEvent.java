package com.cepl.espertest;

public class MyEvent {
    private String type;
    private int id;

    public MyEvent(String type, int id){
        this.type = type;
        this.id = id;
    }

	public int getId() {
		return id;
    }
    
    public String getType() {
		return type;
    }
    
    @Override
    public String toString(){
        return type + "(id=" + id + ")";
    }
}
