package com.neohope.kks.demo.clickstreamenrich.model;

/**
 * 用户查询了那些关键词
 * @author Hansen
 */
public class Search {
    int userID;
    String searchTerms;

    public Search(int userID, String searchTerms) {
        this.userID = userID;
        this.searchTerms = searchTerms;
    }

    public int getUserID() {
        return userID;
    }

    public String getSearchTerms() {
        return searchTerms;
    }
}
