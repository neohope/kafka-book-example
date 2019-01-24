package com.neohope.kks.demo.stockstats.model;

/**
 * 交易状态，计数，和，最小值，平均值等
 * @author Hansen
 */
public class TradeStats {

	public String type;
    public String ticker;
    public int countTrades;
    public double sumPrice;
    public double minPrice;
    public double avgPrice;

    public TradeStats add(Trade trade) {

        if (trade.type == null || trade.ticker == null)
            throw new IllegalArgumentException("Invalid trade to aggregate: " + trade.toString());

        if (this.type == null)
            this.type = trade.type;
        if (this.ticker == null)
            this.ticker = trade.ticker;

        if (!this.type.equals(trade.type) || !this.ticker.equals(trade.ticker))
            throw new IllegalArgumentException("Aggregating stats for trade type " + this.type + " and ticker " + this.ticker + " but recieved trade of type " + trade.type +" and ticker " + trade.ticker );

        if (countTrades == 0) this.minPrice = trade.price;
        
        this.countTrades = this.countTrades+1;
        this.sumPrice = this.sumPrice + trade.price;
        this.minPrice = this.minPrice < trade.price ? this.minPrice : trade.price;

        return this;
    }

    public TradeStats computeAvgPrice() {
        this.avgPrice = this.sumPrice / this.countTrades;
        return this;
    }
}
