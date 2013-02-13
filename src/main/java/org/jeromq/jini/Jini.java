/*
 =========================================================================
     Jini.java

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

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.jeromq.ZContext;
import org.jeromq.ZMQ.Poller;
import org.jeromq.ZMQ.Socket;
import org.jeromq.ZMsg;
import org.jeromq.ZThread;

public class Jini {

    private final ZContext ctx;
    private final JiniAgent agent;
    private Socket pipe;

    private final ConcurrentHashMap <String, List <IJiniHandler>> callbacks;
    
    public Jini (int ioThreads, Properties conf) 
    {
        ctx = new ZContext (ioThreads);
        agent = new JiniAgent (conf);
        
        callbacks = new ConcurrentHashMap <String, List <IJiniHandler>> ();
	}

    public void destory ()
    {
        agent.destory ();
        ctx.destroy ();
    }
    
    public void registerCommand (String command, IAgentHandler handler)
    {
        agent.registerCommand (command, handler);
    }
    
    public void registerEventHandler (IAgentHandler handler)
    {
        agent.registerEventHandler (handler);
    }
    
    public void registerCommandCallback (String command, IJiniHandler handler)
    {
        callbacks.putIfAbsent (command, new CopyOnWriteArrayList <IJiniHandler> ());
        callbacks.get (command).add (handler);
    }

    public void registerCallback (String signature, IAgentHandler handler)
    {
        agent.registerCallback (signature.hashCode (), handler);
    }
    

    public void registerTimer (long interval, IAgentHandler handler)
    {
        if (handler == null)
            return;
        
        agent.registerTimer (interval, handler);
    }

    public synchronized boolean sendCommand (String command, ZMsg msg) 
    {
        pipe.sendMore (command);
        return msg.send (pipe);
    }

    public void loop ()
    {
        pipe = ZThread.fork (ctx, agent);

        assert (pipe != null);

        Poller poller = ctx.getContext ().poller ();

        poller.register (pipe, Poller.POLLIN);
        
        while (!Thread.currentThread ().isInterrupted ()) {

            if (poller.poll (-1) < 0)
                break;

            if (poller.pollin (0)) {
                handleCommandCallback ();
            }
        }
    }

    private void handleCommandCallback ()
    {
        ZMsg msg = ZMsg.recvMsg (pipe);
        String command = msg.popString ();
        
        List <IJiniHandler> handlers = callbacks.get (command);
        List <IJiniHandler> all = callbacks.get ("*");
        
        if (handlers != null) {
            for (IJiniHandler handler : handlers) {
                handler.processCommandCallback (this, command, msg);
            }
        }
        if (all != null) {
            for (IJiniHandler handler : all) {
                handler.processCommandCallback (this, command, msg);
            }
        }
    }
        
}