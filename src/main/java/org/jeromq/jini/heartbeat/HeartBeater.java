/*
 =========================================================================
     HeartBeater.java

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

package org.jeromq.jini.heartbeat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.jeromq.ZMsg;
import org.jeromq.jini.IAgentHandler;
import org.jeromq.jini.JiniAgent;
import org.jeromq.jini.JiniEvent;
import org.jeromq.jini.JiniMsg;
import org.jeromq.jini.JiniPeer;

public class HeartBeater implements IAgentHandler
{
    private JiniAgent agent;
    
    private String [] addrs;
    private long evasive;
    private long expired;
    
    public HeartBeater (String hosts, long evasive, long expired, String ... args)
    {
        addrs = hosts.split (",");
        this.evasive = evasive;
        this.expired = expired;
    }
    
    @Override
    public void initialize (JiniAgent agent, Object ... args)
    {
        this.agent = agent;
        for (String addr : addrs)
            agent.connectPeer (addr);
    }
    
    @Override
    public int getSignature ()
    {
        return HeartBeatMsg.HEART_BEAT_MSG_SIGNATURE;
    }


    @Override
    public void processCommand (String command, ZMsg msg)
    {

    }
    
    @Override
    public void onEvent (JiniEvent event, Object ... params)
    {
        
    }

    @Override
    public long processTimer (long interval)
    {
        ArrayList <String> expired = new ArrayList <String> ();
        
        for (JiniPeer peer : agent.getPeers ().values ()) {
            if (peer.expiredAt () > 0 && 
                    System.currentTimeMillis () >= peer.expiredAt ()) {
                expired.add (peer.getEndpoint ());
                agent.onEvent (JiniEvent.PEER_EXIT, peer.getEndpoint ());
            } else if (System.currentTimeMillis () >= peer.evasiveAt ()) {
                peer.send (new HeartBeatMsg.Ping (peer.incSequence ()));
            }
        }
        if (!expired.isEmpty ())
            agent.removePeer (expired.toArray (new String [0]));
        
        return interval;
    }

    @Override
    public void processCallback (JiniMsg msg)
    {
        String endpoint = msg.getIdentityString ();
        JiniPeer peer = agent.findPeer (endpoint);
        
        if (peer == null && msg.getId () == HeartBeatMsg.PING)
            peer = agent.connectPeer (endpoint);
        if (peer == null)
            return;
        
        switch (msg.getId ()) {
        case HeartBeatMsg.PING:
            peer.send (new HeartBeatMsg.PingOk (peer.incSequence ()));
            break;
            
        case HeartBeatMsg.PING_OK:
            if (!peer.getReady ()) {
                peer.setReady (true);
                sendHello ();
                agent.onEvent (JiniEvent.PEER_ENTER, peer.getEndpoint ());
            }
            break;
            
        case HeartBeatMsg.HELLO:
            HeartBeatMsg.Hello hello = (HeartBeatMsg.Hello) msg;
            List <String> peers = hello.getPeers ();
            for (String peerAddr : peers)
            {
                if (peerAddr.equals (agent.getAddress ()))
                    continue;
                
                JiniPeer peer_ = agent.findPeer (peerAddr);
                if (peer_ == null) {
                    //  Other node knows new peer information
                    agent.connectPeer (peerAddr);
                }
            }
            break;
        }
        peer.refresh (evasive, expired);
    }

    /**
     * Distribute peer information
     */
    private void sendHello ()
    {
        HeartBeatMsg.Hello msg = new HeartBeatMsg.Hello ();
        for (Map.Entry <String, JiniPeer> entry : agent.getPeers ().entrySet ()) {
            if (!entry.getValue ().getReady ())
                continue;
            
            msg.appendPeers (entry.getKey ()); 
        }
        msg.setUnversionedHeaders (agent.getHeaders ());
        
        for (JiniPeer peer : agent.getPeers ().values ()) 
        {
            if (!peer.getReady ())
                continue;
            msg.setSequence (peer.incSequence ());
            peer.send (msg);            
        }
        
    }

    @Override
    public void destroy ()
    {
        
    }

}
