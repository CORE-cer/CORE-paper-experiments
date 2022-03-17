/*
 * Copyright (c) 2011, Regents of the University of Massachusetts Amherst 
 * All rights reserved.

 * Redistribution and use in source and binary forms, with or without modification, are permitted 
 * provided that the following conditions are met:

 *   * Redistributions of source code must retain the above copyright notice, this list of conditions 
 * 		and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright notice, this list of conditions 
 * 		and the following disclaimer in the documentation and/or other materials provided with the distribution.
 *   * Neither the name of the University of Massachusetts Amherst nor the names of its contributors 
 * 		may be used to endorse or promote products derived from this software without specific prior written 
 * 		permission.

 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR 
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS 
 * FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE 
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES 
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; 
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, 
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF 
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/
package edu.umass.cs.sase.UI;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import edu.umass.cs.sase.engine.ConfigFlags;
import edu.umass.cs.sase.engine.Engine;
import edu.umass.cs.sase.engine.EngineController;
import edu.umass.cs.sase.engine.Profiling;
import edu.umass.cs.sase.stream.*;
import net.sourceforge.jeval.EvaluationException;


/**
 * The interface
 * @author haopeng
 *
 */
public class CommandLineUI {

	public static boolean memoryTest = false;
	public static int max_events = 0;

	/**
	 * The main entry to run the engine under command line
	 * 
	 * @param args the inputs 
	 * 0: the nfa file location 
	 * 1: the stream config file
	 * 2: print the results or not (1 for print, 0 for not print)
	 * 3: use sharing techniques or not, ("sharingengine" for use, nothing for not use)
	 */
	public static void main(String[] args) throws CloneNotSupportedException, IOException, EvaluationException {

		long total = Runtime.getRuntime().totalMemory();
//		Profiling.maxMemTotal = Profiling.avgMemTotal = total;
//		System.gc();
		long used = total - Runtime.getRuntime().freeMemory();
//		Profiling.maxMemUsed = Profiling.avgMemUsed = used;

		String streamConfigFile = "test.stream";

		ConfigFlags.printResults = true;
		memoryTest = Boolean.parseBoolean(args[3]);
		boolean double_letter = Boolean.parseBoolean(args[4]);
		int buy_sell = 0;
		if (memoryTest) {
			System.gc();
			Profiling.initialMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
		}
		String nfaFileLocation = args[0];
		String streamFile = args[1];
		boolean consumption = Boolean.parseBoolean(args[2]);
		if(args.length > 5){
			max_events = Integer.parseInt(args[5]);
		}
		if(args.length > 6){
			buy_sell = Integer.parseInt(args[6]);
		}
		if(args.length > 7){
			Engine.timeout = Integer.parseInt(args[7]);
		}
		Stream stream;

		if (memoryTest) {
			stream = new MemoryStream(1, streamFile, max_events, buy_sell, double_letter);
		} else {
			ArrayList<Event> events = new ArrayList<>();
			FileReader file = new FileReader(streamFile);
			BufferedReader fileStream = new BufferedReader(file);

			String line;
			int id;

			int eventsNum = 0;
			while ((line = fileStream.readLine()) != null){
				if (max_events != 0 && eventsNum >= max_events) {
					break;
				}
				if (buy_sell == 1) {
					events.add(BuySellEvent.getEventFromString(line));
				}
				else if (buy_sell == 2) {
					events.add(SmartHomeEvent.getEventFromString(line));
				}
				else if (buy_sell == 3) {
					events.add(TaxiEvent.getEventFromString(line));
				}
				else if (double_letter) {
					id = Integer.parseInt(line.substring(6, line.length() - 1));
					events.add(new ABCEvent(id, line.substring(0, 2)));
				} else {
					id = Integer.parseInt(line.substring(5, line.length() - 1));
					events.add(new ABCEvent(id, line.substring(0, 1)));
				}
				eventsNum++;
			}

			stream = new Stream(1);
			stream.setEvents(events.toArray(new Event[0]));
		}


		EngineController myEngineController = new EngineController();

//		if(engineType != null){
//			myEngineController = new EngineController(engineType);
//		}
		long startCompilationTime = System.nanoTime();
		myEngineController.setNfa(nfaFileLocation);
		long compilationTime = System.nanoTime() - startCompilationTime;
				
			//repreat multiple times for a constant performance
		myEngineController.initializeEngine();

		myEngineController.setInput(stream);
		myEngineController.setConsumption(consumption);
		myEngineController.runEngine();

		if (!memoryTest) {
			if (buy_sell > 0) {
				System.out.print((double)(System.nanoTime() - Engine.start)/1000000000 + ",");
			}
			System.out.print(Profiling.numberOfEvents + ",");
		}
		Profiling.printProfiling();
	}
}
