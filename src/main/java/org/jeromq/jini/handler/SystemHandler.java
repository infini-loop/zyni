/*
 =========================================================================
     SystemHandler.java

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

package org.jeromq.jini.handler;

import org.jeromq.ZMsg;
import org.jeromq.jini.IAgentHandler;
import org.jeromq.jini.JiniAgent;
import org.jeromq.jini.JiniEvent;
import org.jeromq.jini.JiniMsg;

public class SystemHandler implements IAgentHandler
{
    private JiniAgent agent;
    
    @Override
    public void initialize (JiniAgent agent, Object ... args)
    {
        this.agent = agent;
    }

    @Override
    public int getSignature ()
    {
        return 0;
    }

    @Override
    public void processCommand (String command, ZMsg msg)
    {
        if (command.equals ("HEADER.PUT")) {
            agent.putHeader (msg.popString (), msg.popString ());
        }
    }

    @Override
    public void onEvent (JiniEvent event, Object ... params)
    {
        switch (event) {
        case PEER_ENTER:
            agent.sendCommand ("PEER.ENTER", (String) params [0]);
            break;
        case PEER_EXIT:
            agent.sendCommand ("PEER.EXIT", (String) params [0]);
            break;
        default:
            break;
        }

    }

    @Override
    public void processCallback (JiniMsg msg)
    {

    }

    @Override
    public long processTimer (long interval)
    {
        return 0;
    }

    @Override
    public void destroy ()
    {
        
    }

}
