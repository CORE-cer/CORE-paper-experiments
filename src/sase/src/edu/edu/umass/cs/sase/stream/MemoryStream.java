package edu.umass.cs.sase.stream;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class MemoryStream extends Stream {

    private String fileName;
    private BufferedReader fileStream;
    private int maxEvents;
    private int eventsNum;
    private int buy_sell;
    private boolean double_letter;

    /**
     * Constructor
     *
     * @param size the size of the stream
     */
    public MemoryStream(int size, String fileName, int maxEvents, int buy_sell, boolean double_letter) throws FileNotFoundException {
        super(size);
        FileReader file = new FileReader(fileName);
        fileStream = new BufferedReader(file);
        this.maxEvents = maxEvents;
        this.buy_sell = buy_sell;
        this.double_letter = double_letter;
    }

    public Event popEvent(){
        eventsNum++;
        try {
            String line = fileStream.readLine();
            if (line != null){
                if (maxEvents != 0 && eventsNum >= maxEvents) {
                    return null;
                }
                if (buy_sell == 1) {
                    return BuySellEvent.getEventFromString(line);
                }
                else if (buy_sell == 2) {
                    return SmartHomeEvent.getEventFromString(line);
                }
                else if (buy_sell == 3) {
                    return TaxiEvent.getEventFromString(line);
                }
                else if (double_letter) {
                    int id = Integer.parseInt(line.substring(6, line.length() - 1));
                    return new ABCEvent(id, line.substring(0, 2));
                } else {
                    int id = Integer.parseInt(line.substring(5, line.length() - 1));
                    return new ABCEvent(id, line.substring(0, 1));
                }
            }
        } catch (IOException ignored) {
        }
        return null;
    }
}
