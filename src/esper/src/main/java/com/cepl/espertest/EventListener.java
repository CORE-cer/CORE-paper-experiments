package com.cepl.espertest;

import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.runtime.client.EPRuntime;
import com.espertech.esper.runtime.client.EPStatement;
import com.espertech.esper.runtime.client.UpdateListener;

public class EventListener implements UpdateListener{
    public static long totalTime = 0;   
    public static long totalMatches = 0;
    public static int count = 0;
    public static int limit = 10;

    public static String getEPL(){
        // return "select a, b, c from pattern @DiscardPartialsOnMatch" +
        //        "[ every ( [1:] ( [1:] a=MyEvent(type='A') until b=MyEvent(type='B') ) until c=MyEvent(type='C') ) ]";
        return "select a, b, c from MyEvent "
                   + "match_recognize ( "
                   + "    measures A as a, B as b, C as c"
                   + "    all matches "
                   + "    pattern ( A s* B s* C ) "
                   + "    define "
                   + "        A as A.type = \"A\" and A.id < 100, "
                   + "        B as B.type = \"B\" and B.id >= 0, "
                   + "        C as C.type = \"C\")"; // and C.id < 50, "
                //    + "        D as D.type = \"D\")";
    }

    @Override
    public void update(EventBean[] newData, EventBean[] oldData, EPStatement epStatement, EPRuntime epRuntime) {
        // when sending count
        //         System.out.println("MATCH: " + newData[0].get("count") + " diferent outputs");

        count = 0;
        // when sending all results
        long startTime = System.nanoTime();
        //        System.err.println("Match found!:");
        for (EventBean bean: newData){
            if (count >= limit) {
                break;
            }
            totalMatches++;
            //             for (MyEvent a: (MyEvent[]) bean.get("a")){
            //                 System.err.print(a + " ");
            //             }
            try {
//                System.err.print(bean.get("s1") + " ");
//                System.err.print(bean.get("b1") + " ");
//                System.err.print(bean.get("b2") + " ");
//                //                if (bean.get("b1") != null)
//                //                    for (BuySellEvent a: (BuySellEvent[]) bean.get("b1")){
//                //                        System.err.print(a + " ");
//                //                    }
//                //                if (bean.get("b2") != null)
//                //                    for (BuySellEvent a: (BuySellEvent[]) bean.get("b2")){
//                //                        System.err.print(a + " ");
//                //                    }
//                System.err.print(bean.get("s2") + " ");
                    System.err.print(bean.get("aa") + " ");
                    System.err.print(bean.get("ab") + " ");
                    System.err.print(bean.get("ac") + " ");
                    System.err.print(bean.get("ad") + " ");
                    System.err.print(bean.get("ae") + " ");
                    System.err.print(bean.get("af") + " ");
                    System.err.print(bean.get("ag") + " ");
                    System.err.print(bean.get("ah") + " ");
                    System.err.print(bean.get("ai") + " ");
                    System.err.print(bean.get("aj") + " ");
                    System.err.print(bean.get("ak") + " ");
                    System.err.print(bean.get("al") + " ");
                    System.err.print(bean.get("am") + " ");
                    System.err.print(bean.get("an") + " ");
                    System.err.print(bean.get("ao") + " ");
                    System.err.print(bean.get("ap") + " ");
                    System.err.print(bean.get("aq") + " ");
                    System.err.print(bean.get("ar") + " ");
                    System.err.print(bean.get("au") + " ");
                    System.err.print(bean.get("av") + " ");
                    System.err.print(bean.get("aw") + " ");
                    System.err.print(bean.get("ax") + " ");
                    System.err.print(bean.get("ay") + " ");
                    System.err.print(bean.get("az") + " ");
                    System.err.print(bean.get("ba") + " ");
                    System.err.print(bean.get("bb") + " ");
                    System.err.print(bean.get("bc") + " ");
                    System.err.print(bean.get("bd") + " ");
                    System.err.print(bean.get("be") + " ");
                    System.err.print(bean.get("bf") + " ");
                    System.err.print(bean.get("bg") + " ");
                    System.err.print(bean.get("bh") + " ");
                    System.err.print(bean.get("bi") + " ");
                    System.err.print(bean.get("bj") + " ");
                    System.err.print(bean.get("bk") + " ");
                    System.err.print(bean.get("bl") + " ");
                    System.err.print(bean.get("bm") + " ");
                    System.err.print(bean.get("bn") + " ");
            } catch (Throwable ignored) {
                totalTime += System.nanoTime() - startTime;
                startTime = System.nanoTime();
            }
            System.err.println();
            //             System.err.println("[ " + bean.get("a") + " " + bean.get("b") + " " + bean.get("c") + " " + bean.get("d") + " ]" );
            count++;

        }

        //         System.out.println("- MATCH TRIGGERED AT B(id=" + newData[0].get("b_id") + ").");
        totalTime += System.nanoTime() - startTime;
    }
}
