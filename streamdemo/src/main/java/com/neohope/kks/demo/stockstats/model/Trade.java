package com.neohope.kks.demo.stockstats.model;

/**
 * 一次交易
 * @author Hansen
 */
public class Trade {

    String type;
    String ticker;
    double price;
    int size;

    public Trade(String type, String ticker, double price, int size) {
        this.type = type;
        this.ticker = ticker;
        this.price = price;
        this.size = size;
    }

    public double getPrice() {
        return price;
    }

    @Override
    public String toString() {
        return "Trade{" +
                "type='" + type + '\'' +
                ", ticker='" + ticker + '\'' +
                ", price=" + price +
                ", size=" + size +
                '}';
    }
}
