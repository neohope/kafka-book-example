package com.neohope.kk.kkdemo.beans;

public class TCustomer {
	private int customerID;
	private String customerName;
	
	public TCustomer(int ID, String name){
		this.customerID=ID;
		this.customerName=name;
	}
	
	public int getID(){
		return customerID;
	}
	
	public String getName(){
		return customerName;
	}
}
