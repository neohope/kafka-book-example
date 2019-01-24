package com.neohope.kks.demo.clickstreamenrich.model;

/**
 * 用户活动
 * @author Hansen
 */
public class UserActivity {
	public int userId;
    public String userName;
    public String zipcode;
    public String[] interests;
    public String searchTerm;
    public String page;

    public UserActivity(int userId, String userName, String zipcode, String[] interests, String searchTerm, String page) {
        this.userId = userId;
        this.userName = userName;
        this.zipcode = zipcode;
        this.interests = interests;
        this.searchTerm = searchTerm;
        this.page = page;
    }

    public UserActivity updateSearch(String searchTerm) {
        this.searchTerm = searchTerm;
        return this;
    }
}
