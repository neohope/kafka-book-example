package com.neohope.kks.demo.clickstreamenrich.model;

/**
 * 用户看了那些网页
 * @author Hansen
 */
public class PageView {
    int userID;
    String page;

    public PageView(int userID, String page) {
        this.userID = userID;
        this.page = page;
    }

    public int getUserID() {
        return userID;
    }

    public String getPage() {
        return page;
    }
}
