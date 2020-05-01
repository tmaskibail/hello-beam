package com.maskibail.data.model;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;

public class AvocadoSale implements Serializable {
    private Long id;
    private Date date;
    private BigDecimal averagePrice;
    private Double totalVolume;
    private Double volumeWithPLU4046;
    private Double volumeWithPLU4225;
    private Double volumeWithPLU4770;
    private Double totalBags;
    private Double smallBags;
    private Double largeBags;
    private Double xLargeBags;
    private String type;
    private Long year;
    private String region;

    @Override
    public String toString() {
        return "AvocadoSale{" +
                "id=" + id +
                ", date=" + date +
                ", averagePrice=" + averagePrice +
                ", totalVolume=" + totalVolume +
                ", volumeWithPLU4046=" + volumeWithPLU4046 +
                ", volumeWithPLU4225=" + volumeWithPLU4225 +
                ", volumeWithPLU4770=" + volumeWithPLU4770 +
                ", totalBags=" + totalBags +
                ", smallBags=" + smallBags +
                ", largeBags=" + largeBags +
                ", xLargeBags=" + xLargeBags +
                ", type='" + type + '\'' +
                ", year=" + year +
                ", region='" + region + '\'' +
                '}';
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public BigDecimal getAveragePrice() {
        return averagePrice;
    }

    public void setAveragePrice(BigDecimal averagePrice) {
        this.averagePrice = averagePrice;
    }

    public Double getTotalVolume() {
        return totalVolume;
    }

    public void setTotalVolume(Double totalVolume) {
        this.totalVolume = totalVolume;
    }

    public Double getVolumeWithPLU4046() {
        return volumeWithPLU4046;
    }

    public void setVolumeWithPLU4046(Double volumeWithPLU4046) {
        this.volumeWithPLU4046 = volumeWithPLU4046;
    }

    public Double getVolumeWithPLU4225() {
        return volumeWithPLU4225;
    }

    public void setVolumeWithPLU4225(Double volumeWithPLU4225) {
        this.volumeWithPLU4225 = volumeWithPLU4225;
    }

    public Double getVolumeWithPLU4770() {
        return volumeWithPLU4770;
    }

    public void setVolumeWithPLU4770(Double volumeWithPLU4770) {
        this.volumeWithPLU4770 = volumeWithPLU4770;
    }

    public Double getTotalBags() {
        return totalBags;
    }

    public void setTotalBags(Double totalBags) {
        this.totalBags = totalBags;
    }

    public Double getSmallBags() {
        return smallBags;
    }

    public void setSmallBags(Double smallBags) {
        this.smallBags = smallBags;
    }

    public Double getLargeBags() {
        return largeBags;
    }

    public void setLargeBags(Double largeBags) {
        this.largeBags = largeBags;
    }

    public Double getxLargeBags() {
        return xLargeBags;
    }

    public void setxLargeBags(Double xLargeBags) {
        this.xLargeBags = xLargeBags;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Long getYear() {
        return year;
    }

    public void setYear(Long year) {
        this.year = year;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }
}
