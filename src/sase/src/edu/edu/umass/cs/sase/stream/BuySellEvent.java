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
public class BuySellEvent implements Event {

	private static Map<String, Integer> nameMap = new HashMap<>();
	private static int nameCounter = 0;
	private static int counter = 1;
	/**
	 * Event id
	 */
	int id;

	int my_id;

	/**
	 * Event timestamp
	 */
	int timestamp;

	/**
	 * event type
	 */
	String eventType;

	/**
	 * Price, an attribute
	 */
	double price;

	int volume;
	String name;
	int stock_time;

	/**
	 * Constructor
	 */
	public BuySellEvent(int i, int ts, String et, String n, int vol, double p, int st){
		id = i;
		timestamp = ts;
		eventType = et;
		price = p;
		name = n;
		volume = vol;
		stock_time = st;
	}

	public BuySellEvent(int id, String et){
		this.id = counter++;
		my_id = id;
		// timestamp = ts;
		eventType = et;
		// price = p;
		
	}

	private BuySellEvent() {}
	
	/**
	 * @return the cloned event
	 */
	public Object clone(){
		BuySellEvent o = null;
		try {
			o = (BuySellEvent)super.clone();
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
		return stock_time;
	}

	/**
	 * @param timestamp the timestamp to set
	 */
	public void setTimestamp(int timestamp) {
		this.timestamp = timestamp;
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
	public double getPrice() {
		return price;
	}

	/**
	 * @param price the price to set
	 */
	public void setPrice(double price) {
		this.price = price;
	}

	public String toString(){
		// return "ID="+ id + "\tTimestamp=" + timestamp
		// 	+ "\tEventType=" + eventType + "\tPrice =" + price;
		return eventType + "(id=" + my_id + " name=" + name + ")";
	}

	/* (non-Javadoc)
	 * @see edu.umass.cs.sase.mvc.model.Event#getAttributeByName(java.lang.String)
	 */
	public int getAttributeByName(String attributeName) {

		if(attributeName.equalsIgnoreCase("id"))
			return this.id;
		if(attributeName.equalsIgnoreCase("timestamp"))
			return this.stock_time;
		if(attributeName.equalsIgnoreCase("price"))
			return (int) (this.price*100);
		if(attributeName.equalsIgnoreCase("volume"))
			return this.volume;
		if(attributeName.equalsIgnoreCase("stock_time"))
			return this.stock_time;
		if(attributeName.equalsIgnoreCase("name"))
			return nameMap.get(name);

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
		if(attributeName.equalsIgnoreCase("price"))
			return price;
		return -1;
	}


	public void setName(String name) {
		this.name = name;
	}

	public int getMy_id() {
		return my_id;
	}

	public void setVolume(int volume) {
		this.volume = volume;
	}

	public void setStock_time(int stock_time) {
		this.stock_time = stock_time;
	}

	public void setMy_id(int my_id) {
		this.my_id = my_id;
	}

	static public BuySellEvent getEventFromString(String data) {
		BuySellEvent toReturn = new BuySellEvent();
		int offset = 0;
		if (data.startsWith("B")){
			offset += 3;
		} else {
			offset += 4;
		}
		toReturn.setEventType(data.substring(0, offset));
		data = data.split("[()]")[1];
		List<String> data_values = Arrays.asList(data.split(","));
		toReturn.setMy_id(Integer.parseInt(data_values.get(0).split("=")[1]));
		String name = data_values.get(1).split("=")[1];
		if (nameMap.get(name) == null) {
			nameMap.put(name, nameCounter);
			nameCounter++;
		}
		toReturn.setName(name);
		toReturn.setVolume(Integer.parseInt(data_values.get(2).split("=")[1]));
		toReturn.setPrice(Double.parseDouble(data_values.get(3).split("=")[1]));
		toReturn.setStock_time(Integer.parseInt(data_values.get(4).split("=")[1]));

		toReturn.setId(counter++);
		return toReturn;
	}
}
