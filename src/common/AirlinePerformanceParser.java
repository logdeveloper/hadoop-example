package common;

import jdk.internal.org.objectweb.asm.tree.TryCatchBlockNode;
import org.apache.hadoop.io.Text;

/**
 * The Class .
 */
public class AirlinePerformanceParser {

    private int year;
    private int month;

    private int arriveDelayTime = 0;
    private int departureDelayTime = 0;
    private int distance =0;

    private boolean arriveDelayAvailable = true;
    private boolean departureDelayAvailable = true;
    private boolean distanceAvailable = true;

    private String uniqueCarrier;

    public AirlinePerformanceParser(Text text){
        try{
            String[] colums = text.toString().split(",");

            year = Integer.parseInt(colums[0]);

            month = Integer.parseInt(colums[1]);

            uniqueCarrier = colums[8];

            if(!colums[15].equals("NA")){
                departureDelayTime = Integer.parseInt(colums[15]);
            }else {
                departureDelayAvailable = false;
            }

            if(!colums[14].equals("NA")){
                arriveDelayTime = Integer.parseInt(colums[14]);
            }else {
                arriveDelayAvailable = false;
            }

            if(!colums[18].equals("NA")){
                distance = Integer.parseInt(colums[14]);
            }else {
                distanceAvailable = false;
            }

        }catch (Exception e){
            System.out.println("Error parsing a record :"+ e.getMessage());
        }
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getArriveDelayTime() {
        return arriveDelayTime;
    }

    public void setArriveDelayTime(int arriveDelayTime) {
        this.arriveDelayTime = arriveDelayTime;
    }

    public int getDepartureDelayTime() {
        return departureDelayTime;
    }

    public void setDepartureDelayTime(int departureDelayTime) {
        this.departureDelayTime = departureDelayTime;
    }

    public int getDistance() {
        return distance;
    }

    public void setDistance(int distance) {
        this.distance = distance;
    }

    public boolean isArriveDelayAvailable() {
        return arriveDelayAvailable;
    }

    public void setArriveDelayAvailable(boolean arriveDelayAvailable) {
        this.arriveDelayAvailable = arriveDelayAvailable;
    }

    public boolean isDepartureDelayAvailable() {
        return departureDelayAvailable;
    }

    public void setDepartureDelayAvailable(boolean departureDelayAvailable) {
        this.departureDelayAvailable = departureDelayAvailable;
    }

    public boolean isDistanceAvailable() {
        return distanceAvailable;
    }

    public void setDistanceAvailable(boolean distanceAvailable) {
        this.distanceAvailable = distanceAvailable;
    }

    public String getUniqueCarrier() {
        return uniqueCarrier;
    }

    public void setUniqueCarrier(String uniqueCarrier) {
        this.uniqueCarrier = uniqueCarrier;
    }
}
