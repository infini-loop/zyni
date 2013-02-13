/*
 *     =========================================================================
 *     Zyni.java
 *
 *     -------------------------------------------------------------------------
 *     Copyright (c) 2012-2013 InfiniLoop Corporation
 *     Copyright other contributors as noted in the AUTHORS file.
 *
 *     This file is part of Zyni, an open-source message based application framework.
 *
 *     This is free software; you can redistribute it and/or modify it under
 *     the terms of the GNU Lesser General Public License as published by the
 *     Free Software Foundation; either version 3 of the License, or (at your
 *     option) any later version.
 *
 *     This software is distributed in the hope that it will be useful, but
 *     WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTA-
 *     BILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
 *     Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program. If not, see http://www.gnu.org/licenses/.
 *     =========================================================================
 */

package org.zyni;

import org.zeromq.ZContext;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;
import org.zeromq.ZThread;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class Zyni
{
    private final ZContext ctx;
    private final ZyniAgent agent;
    private Socket pipe;
    private volatile boolean stop = false;

    private final ConcurrentHashMap<String, List<IZyniHandler>> callbacks;

    public Zyni (int ioThreads, Properties conf)
    {
        ctx = new ZContext (ioThreads);
        ctx.setLinger (3000);
        agent = new ZyniAgent (conf);

        callbacks = new ConcurrentHashMap <String, List <IZyniHandler>> ();
    }

    public void destroy ()
    {
        sendCommand (ZyniEvent.SYSTEM_EXIT.toString (), null);
    }

    public void registerCommand (String command, IAgentHandler handler)
    {
        agent.registerCommand (command, handler);
    }

    public void registerEventHandler (IAgentHandler handler)
    {
        agent.registerEventHandler (handler);
    }

    public void registerCommandCallback (String command, IZyniHandler handler)
    {
        callbacks.putIfAbsent (command, new CopyOnWriteArrayList<IZyniHandler> ());
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

    /**
     * Send command to ZyniAgent thread
     * @param command
     * @param msg instance of org.zeromq.ZMsg
     * @return true if send succeeds
     */
    public boolean sendCommand (String command, ZMsg msg)
    {
        if (msg == null)
            return pipe.send (command);

        if (pipe.sendMore (command))
            return msg.send (pipe);

        return false;
    }

    public void loop ()
    {
        pipe = ZThread.fork (ctx, agent);

        assert (pipe != null);

        Poller poller = new Poller (1);

        poller.register (pipe, Poller.POLLIN);

        while (!stop && !Thread.currentThread ().isInterrupted ()) {

            if (poller.poll (-1) < 0)
                break;

            if (poller.pollin (0)) {
                handleCommandCallback ();
            }
        }

        ctx.destroy ();
    }

    private void handleCommandCallback ()
    {
        ZMsg msg = ZMsg.recvMsg (pipe);
        String command = msg.popString ();

        List <IZyniHandler> handlers = callbacks.get (command);
        List <IZyniHandler> all = callbacks.get ("*");

        if (handlers != null) {
            for (IZyniHandler handler : handlers) {
                handler.processCommandCallback (this, command, msg);
            }
        }
        if (all != null) {
            for (IZyniHandler handler : all) {
                handler.processCommandCallback (this, command, msg);
            }
        }

        if (command.equals (ZyniEvent.AGENT_EXIT.toString ()))
        {
            stop = true;
        }
    }
}
