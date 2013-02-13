/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.zyni.heartbeat;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * This FailureDetector comes from Cassandra Gossip implementation
 *
 * This FailureDetector is an implementation of the paper titled
 * "The Phi Accrual Failure Detector" by Hayashibara.
 * Check the paper and the Cassandra.IFailureDetector interface for details.
 */
public class FailureDetector
{
    private static final int SAMPLE_SIZE = 1000;

    private final Map <String, ArrivalWindow> arrivalSamples = new HashMap <String, ArrivalWindow> ();

    private double convictThreshold;
    private long requestTimeout;
    private final long interval;

    public FailureDetector (long interval)
    {
        this (interval, 8.0, 10000);
        assert (interval > 0);
    }
    
    public FailureDetector (long interval, double convictThreshold, long requestTimeout)
    {
        this.interval = interval;
        this.convictThreshold = convictThreshold;
        this.requestTimeout = requestTimeout;
    }

    public void setPhiConvictThreshold (double phi)
    {
        convictThreshold = phi;
    }

    public double getPhiConvictThreshold()
    {
        return convictThreshold;
    }

    public void clear (String endpoint)
    {
        ArrivalWindow heartbeatWindow = arrivalSamples.get (endpoint);
        if (heartbeatWindow != null)
            heartbeatWindow.clear();
    }

    public void report (String endpoint)
    {
        long now = System.currentTimeMillis ();
        ArrivalWindow heartbeatWindow = arrivalSamples.get (endpoint);
        if ( heartbeatWindow == null )
        {
            heartbeatWindow = new ArrivalWindow (SAMPLE_SIZE, interval, requestTimeout);
            arrivalSamples.put (endpoint, heartbeatWindow);
        }
        heartbeatWindow.add (now);
    }

    /**
     *
     * @param endpoint
     * @return true if convict is required
     */
    public boolean interpret (String endpoint)
    {
        ArrivalWindow hbWnd = arrivalSamples.get (endpoint);
        if (hbWnd == null)
        {
            return false;
        }
        long now = System.currentTimeMillis ();
        double phi = hbWnd.phi (now);

        if (phi > getPhiConvictThreshold ())
        {
            return true;
        }
        return false;
    }

    public void remove (String ep)
    {
        arrivalSamples.remove(ep);
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        Set <String> eps = arrivalSamples.keySet();

        sb.append("-----------------------------------------------------------------------");
        for (String ep : eps)
        {
            ArrivalWindow hWnd = arrivalSamples.get(ep);
            sb.append(ep + " : ");
            sb.append(hWnd.toString());
            sb.append(System.getProperty("line.separator"));
        }
        sb.append("-----------------------------------------------------------------------");
        return sb.toString();
    }
}

class ArrivalWindow
{
    private double tLast = 0L;
    private final LinkedBlockingDeque <Double> arrivalIntervals;

    // this is useless except to provide backwards compatibility in phi_convict_threshold,
    // because everyone seems pretty accustomed to the default of 8, and users who have
    // already tuned their phi_convict_threshold for their own environments won't need to
    // change.
    private final double PHI_FACTOR = 1.0 / Math.log(10.0);

    // in the event of a long partition, never record an interval longer than the rpc timeout,
    // since if a host is regularly experiencing connectivity problems lasting this long we'd
    // rather mark it down quickly instead of adapting
    private double MAX_INTERVAL_IN_MS;

    private long gossipInterval;
    
    ArrivalWindow (int size, long gossipInterval, double MAX_INTERVAL_IN_MS)
    {
        arrivalIntervals = new LinkedBlockingDeque <Double> (size);
        this.gossipInterval = gossipInterval;
        this.MAX_INTERVAL_IN_MS = MAX_INTERVAL_IN_MS;
    }

    void add (double value)
    {
        double interArrivalTime;
        if ( tLast > 0L )
        {
            interArrivalTime = (value - tLast);
        }
        else
        {
            interArrivalTime = gossipInterval / 2;
        }
        if (interArrivalTime <= MAX_INTERVAL_IN_MS)
            arrivalIntervals.add(interArrivalTime);
        tLast = value;
    }

    int size ()
    {
        return arrivalIntervals.size ();
    }
    
    double sum ()
    {
        double sum = 0d;
        for (Double interval : arrivalIntervals)
        {
            sum += interval;
        }
        return sum;
    }

    double mean ()
    {
        return size() > 0 ? sum() / size() : 0;
    }

    void clear ()
    {
        arrivalIntervals.clear();
    }

    // see CASSANDRA-2597 for an explanation of the math at work here.
    double phi(long now)
    {
        int size = arrivalIntervals.size();
        double t = now - tLast;
        return (size > 0)
               ? PHI_FACTOR * t / mean()
               : 0.0;
    }

}

