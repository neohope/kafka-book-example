package com.neohope.kks.demo.clickstreamenrich.model;

/**
 * 用户基本信息
 * @author Hansen
 */
public class UserProfile {
    int userID;
    String userName;
    String zipcode;
    String[] interests;

    public UserProfile(int userID, String userName, String zipcode, String[] interests) {
        this.userID = userID;
        this.userName = userName;
        this.zipcode = zipcode;
        this.interests = interests;
    }

    public int getUserID() {
        return userID;
    }

    public UserProfile update(String zipcode, String[] interests) {
        this.zipcode = zipcode;
        this.interests = interests;
        return this;
    }

    public String getUserName() {
        return userName;
    }

    public String getZipcode() {
        return zipcode;
    }

    public String[] getInterests() {
        return interests;
    }
}
