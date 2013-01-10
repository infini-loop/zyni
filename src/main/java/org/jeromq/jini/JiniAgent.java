/*
 =========================================================================
     JiniAgent.java

     -------------------------------------------------------------------------
     Copyright (c) 2012-2012 InfiniLoop Corporation
     Copyright other contributors as noted in the AUTHORS file.

     This file is part of Jini, an open-source message based application framework.

     This is free software; you can redistribute it and/or modify it under
     the terms of the GNU Lesser General Public License as published by the
     Free Software Foundation; either version 3 of the License, or (at your
     option) any later version.

     This software is distributed in the hope that it will be useful, but
     WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTA-
     BILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
     Public License for more details.

     You should have received a copy of the GNU Lesser General Public License
     along with this program. If not, see http://www.gnu.org/licenses/.
     =========================================================================
 */

package org.jeromq.jini;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.jeromq.ZContext;
import org.jeromq.ZMQ;
import org.jeromq.ZMQ.Poller;
import org.jeromq.ZMQ.Socket;
import org.jeromq.jini.JiniMsg.VersionedMap;
import org.jeromq.jini.JiniMsg.VersionedValue;
import org.jeromq.jini.handler.SystemHandler;
import org.jeromq.jini.heartbeat.HeartBeatFactory;
import org.jeromq.ZMsg;
import org.jeromq.ZThread;

public class JiniAgent implements ZThread.IAttachedRunnable {

    private ZContext ctx;
    private Socket pipe;

    private Socket inbox;                   //  Our inbox socket (ROUTER)
    private String address;                 //  Our inbox address

    private final Properties conf;
    private final int sendHWM;
    
    private final TreeSet <TimerInfo> timers;    
    private final Map <String, JiniPeer> peers;
    private final VersionedMap headers;
    private final ConcurrentHashMap <String, List <IAgentHandler>> commands;
    private final List <IAgentHandler> subscribers;
    private final ConcurrentHashMap <Integer, IAgentHandler> callbacks;
    private final HashSet <IAgentHandler> handlers;

    public JiniAgent (Properties conf) 
    {
        this.conf = conf;
        sendHWM = Integer.parseInt (conf.getProperty ("send.hwm", "1000"));
        
        timers = new TreeSet <TimerInfo> (new Comparator <TimerInfo> () {
            @Override
            public int compare (TimerInfo h1, TimerInfo h2)
            {
                return h1.expire <= h2.expire ? -1 : 1;
            }});
        
        headers = new VersionedMap ();
        peers = new HashMap <String, JiniPeer> ();
        commands = new ConcurrentHashMap <String, List <IAgentHandler>> ();
        subscribers = new CopyOnWriteArrayList <IAgentHandler> ();
        callbacks = new ConcurrentHashMap <Integer, IAgentHandler> ();
        
        handlers = new HashSet <IAgentHandler> ();
    }
    
    private boolean initialize (ZContext ctx, Socket pipe) {
        this.ctx = ctx;
        this.pipe = pipe;
        inbox = ctx.createSocket (ZMQ.ROUTER);

        if (inbox == null) //  Interrupted
            return false;

        int port = inbox.bind ("tcp://" + conf.getProperty ("bind"));
        if (port < 0)          //  Interrupted
            return false;

        String host = conf.getProperty ("bind").split (":")[0];
        InetAddress addr;
        try {
            addr = InetAddress.getByName (host);
        } catch (UnknownHostException e) {
            return false;
        }

        address = String.format ("%s:%d", addr.getHostAddress (), port);

        registerSystemHandler ();

        return true;
	}

    public void destory ()
    {
        for (IAgentHandler handler: handlers)
            handler.destroy ();
        
        ctx.destroy ();
    }
    
    private void registerSystemHandler ()
    {
        ArrayList <String> args = new ArrayList <String> ();
        int idx = 0;
        while (true) {
            String arg = conf.getProperty ("heartbeat.args." + idx++);
            if (arg == null)
                break;
            args.add (arg);
        }
        IAgentHandler heartbeat = HeartBeatFactory.getInstance (
                                conf.getProperty ("heartbeat.strategy", "all"),
                                conf.getProperty ("heartbeat.endpoints"),
                                Long.parseLong (conf.getProperty ("heartbeat.evasive", "5000")),
                                Long.parseLong (conf.getProperty ("heartbeat.expired", "10000")),
                                args.toArray (new String [0])
                                );
        IAgentHandler handler = new SystemHandler ();
        registerTimer (
                Long.parseLong (conf.getProperty ("heartbeat.interval", "1000")),
                heartbeat);
        registerCallback (heartbeat.getSignature (), heartbeat);
        
        registerCommand ("HEADER.PUT", handler);
        registerEventHandler (handler);
        
    }

    public void registerCommand (String command, IAgentHandler handler)
    {
        initializeHandler (handler);

        commands.putIfAbsent (command, new CopyOnWriteArrayList <IAgentHandler> ());
        commands.get (command).add (handler);
    }
    
    public void registerEventHandler (IAgentHandler handler)
    {
        initializeHandler (handler);
        subscribers.add (handler);
    }
    
    public void registerCallback (int signature, IAgentHandler handler)
    {
        initializeHandler (handler);
        callbacks.put (signature, handler);
    }

    public void registerTimer (long interval, IAgentHandler handler)
    {
        initializeHandler (handler, interval);

        long now = System.currentTimeMillis ();
        timers.add (new TimerInfo (interval, now, handler));
    }

    private void initializeHandler (IAgentHandler handler, Object ... args)
    {
        if (handlers.add (handler))
            handler.initialize (this, args);
    }

    @Override
    public void run (Object[] args, ZContext ctx, Socket pipe)
    {
        initialize (ctx, pipe);

        Poller poller = ctx.getContext ().poller ();

        poller.register (pipe, Poller.POLLIN);
        poller.register (inbox, Poller.POLLIN);
        
        while (!Thread.currentThread ().isInterrupted ()) {

            long timeout = getTimeout ();
            if (poller.poll (timeout) < 0)
                break;

            if (poller.pollin (0)) {
                handleCommand ();
            }
            
            if (poller.pollin (1)) {
                handleCallback ();
            }
            
            handleTimer ();
        }
        destory ();
    }

    private void handleCommand ()
    {
        ZMsg msg = ZMsg.recvMsg (pipe);
        String command = msg.popString ();
        
        List <IAgentHandler> handlers = commands.get (command);
        
        if (handlers != null) {
            for (IAgentHandler handler : handlers) {
                handler.processCommand (command, msg);
            }
        }
        
        handlers = commands.get ("*");
        if (handlers != null) {
            for (IAgentHandler handler : handlers) {
                handler.processCommand (command, msg);
            }
        }
    }
    
    private void handleCallback ()
    {
        JiniMsg msg = JiniMsg.recv (inbox);
        
        IAgentHandler handler = callbacks.get (msg.getSignature ());
        
        if (handler != null)
            handler.processCallback (msg);
        
    }
    
    public void onEvent (JiniEvent event, Object ... params)
    {
        for (IAgentHandler handler : subscribers) {
            handler.onEvent (event, params);
        }
    }

    /**
     * Timer handler
     */
    private void handleTimer ()
    {
        long now = System.currentTimeMillis ();
        while (true) {
            TimerInfo t = timers.pollFirst ();
            if (t == null)
                break;
            if (t.expire <= now) {
                long next = t.handler.processTimer (t.interval);
                if (next > 0)
                    timers.add (new TimerInfo (next, now + next, t.handler));
            } else {
                timers.add (t);
                break;
            }
        }
    }
    
    /**
     * 
     * @return get the next polling timeout
     */
    private long getTimeout ()
    {
        if (timers.isEmpty ())
            return -1;
        
        long timeout = timers.first ().expire - System.currentTimeMillis ();
        if (timeout < 0)
            timeout = 0;
        
        return timeout;    
    }


    class TimerInfo
    {
        private long interval;
        private long expire;
        private IAgentHandler handler;
        
        private TimerInfo (long interval, long expire, IAgentHandler handler)
        {
            this.interval = interval;
            this.expire = expire;
            this.handler = handler;
        }
    }


    public JiniPeer connectPeer (String endpoint)
    {
        JiniPeer peer = findPeer (endpoint);
        if (peer == null)
            peer = JiniPeer.create (ctx, endpoint, peers);
        peer.connect (address, sendHWM);
        
        return peer;
    }

    public Map <String, JiniPeer> getPeers ()
    {
        return peers;
    }
    
    public String getAddress ()
    {
        return address;
    }

    public String getConfig (String key, String defaultValue)
    {
        return conf.getProperty (key, defaultValue);
    }
    
    public JiniPeer findPeer (String endpoint)
    {
        JiniPeer peer = peers.get (endpoint);

        return peer;
    }

    public void sendCommand (String command, String ... args)
    {
        ZMsg outgoing = new ZMsg ();
        outgoing.add (command);
        
        for (String arg : args)
            outgoing.add (arg);
        
        outgoing.send (pipe);
    }

    public void removePeer (String ... peerIdentities)
    {
        for (String peerIdentity : peerIdentities) {
            JiniPeer peer = peers.remove (peerIdentity);
            peer.destory ();
        }
    }

    public void disconnectPeer (String ... peerIdentities)
    {
        for (String peerIdentity : peerIdentities) {
            findPeer (peerIdentity).disconnect ();
        }
    }



    public VersionedMap getHeaders ()
    {
        return headers;
    }
    
    public VersionedValue putHeader (String key, String value)
    {
        VersionedValue vvalue = headers.get (key);
        if (vvalue == null)
            headers.put (key, new VersionedValue (0, value));
        else
            vvalue.setValue (value);
        
        return vvalue;
    }

}
