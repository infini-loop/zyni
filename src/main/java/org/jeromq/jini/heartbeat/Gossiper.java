/*
     =========================================================================
     Gossiper.java

     -------------------------------------------------------------------------
     Copyright (c) 2012-2013 InfiniLoop Corporation
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

import java.util.*;

import org.jeromq.ZMsg;
import org.jeromq.jini.IAgentHandler;
import org.jeromq.jini.JiniAgent;
import org.jeromq.jini.JiniEvent;
import org.jeromq.jini.JiniMsg;
import org.jeromq.jini.JiniMsg.VersionedMap;
import org.jeromq.jini.JiniMsg.VersionedValue;
import org.jeromq.jini.JiniPeer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Gossip algorithms comes from Cassandra Gossip implementation
 */

public class Gossiper implements IAgentHandler
{
    private final static Logger LOG = LoggerFactory.getLogger (Gossiper.class);

    private JiniAgent agent;
    
    private final String hosts;
    private final Set <String> seeds;
    private final Set <String> liveEndpoints;
    private final Map <String, Long> deadEndpoints;
    private final Map <String, Long> removedEndpoints;
    private final int generation;
    private final long evasive;
    private final long expired;
    private final Random rand;
    private List <String> managingStatus;
    private long interval;
    private FailureDetector failureDetector;

    private State state;
    
    private static class State {

        private final int generation;
        private int version;
        private final String endpoint;
        private boolean alive;
        private boolean managing;
        private final VersionedMap headers;

        public State (GossipMsg.State msg, VersionedMap headers)
        {
            this (msg.getAddressString (), msg.getGeneration (),
                    msg.getMaxVersion (), headers);
        }

        public State (String endpoint, int generation, int version,
                      VersionedMap headers)
        {
            this.generation = generation;
            this.version = version;
            this.endpoint = endpoint;
            this.headers = headers;

            alive = false;
            managing = false;
        }

        public void incVersion ()
        {
            ++version;
        }

        public void setVersion (int version)
        {
            this.version = version;
        }

        public int getMaxVersion ()
        {
            return Math.max (version, headers.getMaxVersion ());
        }

        public boolean isAlive ()
        {
            return alive;
        }
        
        public void setAlive (boolean alive)
        {
            this.alive = alive;
        }

        public GossipMsg.State getMessage ()
        {
            GossipMsg.State state = new GossipMsg.State ();
            state.setAddress (endpoint);
            state.setMaxVersion (getMaxVersion ());
            state.setGeneration (generation);

            return state;
        }

        public boolean isManaging ()
        {
            return managing;
        }

        public void setManaging (boolean managing)
        {
            this.managing = managing;
        }

        public int getVersion ()
        {
            return version;
        }

        public VersionedMap getHeaders ()
        {
            return headers;
        }

        public int getGeneration ()
        {
            return generation;
        }

        public String getEndpoint ()
        {
            return endpoint;
        }
    }
    
    public Gossiper (String hosts, long evasive, long expired, String ... args)
    {
        this.hosts = hosts;
        this.evasive = evasive;
        this.expired = expired;

        seeds = new HashSet <String> ();
        liveEndpoints = new HashSet <String> ();
        deadEndpoints = new HashMap <String, Long> ();
        removedEndpoints = new HashMap <String, Long> ();
        rand = new Random (System.currentTimeMillis ());
        
        generation = Integer.parseInt (args[0]);
    }

    @Override
    public int getSignature ()
    {
        return GossipMsg.GOSSIP_MSG_SIGNATURE;
    }

    @Override
    public void initialize (JiniAgent agent, Object ... args)
    {
        this.agent = agent;
        this.interval = (Long) args [0];
        this.failureDetector = new FailureDetector (interval);

        for (String host : hosts.split (","))
        {
            if (host.equals (agent.getAddress ()))
                continue;
            
            seeds.add(host);
            agent.connectPeer (host);
        }
        
        state = new State (agent.getAddress (), generation, 0, agent.getHeaders ());
        managingStatus = Arrays.asList (agent.getConfig ("MANAGING_STATUS", "").split (","));
    }

    @Override
    public long processTimer (long interval)
    {
        //  Update the local heartbeat counter.
        state.incVersion ();

        Map <String, JiniPeer> peers = agent.getPeers ();
        if (peers.size () > 0)
        {
            GossipMsg.Ping msg = new GossipMsg.Ping ();
            msg.appendDigest (state.getMessage ());

            for (JiniPeer peer : peers.values ()) {
                State peerState = (State) peer.getAttached ();
                if (peerState != null)
                    msg.appendDigest (peerState.getMessage ());
            }

            //  Gossip to some random live member
            boolean seed = sendGossip (msg, liveEndpoints);

            //  Gossip to some unreachable member with some probability to check if he is back up
            sendGossipDead (msg);

            if (!seed || liveEndpoints.size() < seeds.size())
                sendGossip (msg, seeds);

            checkState ();
        }
        
        return interval;
    }
    
    private void sendGossipDead (GossipMsg.Ping msg)
    {
        double liveCount = liveEndpoints.size();
        double unreachableCount = deadEndpoints.size();
        if (unreachableCount > 0)
        {
            //  Based on some probability
            double prob = unreachableCount / (liveCount + 1);
            double randDbl = rand.nextDouble();
            if ( randDbl < prob )
                sendGossip (msg, deadEndpoints.keySet ());
        }

    }

    private boolean sendGossip (GossipMsg.Ping msg, Set <String> peers)
    {
        int size = peers.size ();
        if (size == 0)
            return false;

        int idx = rand.nextInt (size);
        for (String endpoint: peers) {
            if (idx-- == 0) {
                if (LOG.isDebugEnabled ())
                    LOG.debug ("{} sends gossip to {}", state.getEndpoint (), endpoint);
                agent.findPeer (endpoint).send (msg);
                return seeds.contains (endpoint);
            }
        }
        return false;
    }

    private void checkState ()
    {
        long now = System.currentTimeMillis();

        Map <String, JiniPeer> peers = agent.getPeers ();
        for (Map.Entry <String, JiniPeer> entry : peers.entrySet ())
        {
            String endpoint = entry.getKey ();
            JiniPeer peer = entry.getValue ();
            State peerState = (State) peer.getAttached ();

            if (peerState == null) //  No handshake yet
                continue;

            if (failureDetector.interpret (endpoint)
                    && peerState.isAlive () && !peerState.isManaging ()) {
                LOG.info ("convict {}", endpoint);
                markDead (peerState);
                continue;
            }

            if (peerState.isAlive () || peer.getSpecial ())
                continue;

            //  Check if evasive
            if (!peerState.isManaging ()
                    && (now > peer.evasiveAt ()))
            {
                LOG.info ("evasive {}", endpoint);
                removeEndpoint (endpoint);
            }

            //  Check if expired
            if (now > peer.expiredAt ())
            {
                LOG.info ("expired {}", endpoint);
                removeEndpoint (endpoint);
            }
        }

        Iterator <Map.Entry <String, Long>> it = removedEndpoints.entrySet ().iterator ();
        while (it.hasNext ()) {
            Map.Entry <String, Long> entry = it.next ();
            if (now - entry.getValue () > evasive * 2) {
                agent.removePeer (entry.getKey ());
                it.remove ();
            }
        }

    }

    private boolean isPeerManaging (JiniPeer peer)
    {
        String value = peer.getHeaders ().getValue ("STATUS");
        if (value == null)
            return false;

        String status = value.split (",") [0];

        return managingStatus.contains (status);
    }

    private void removeEndpoint (String endpoint)
    {
        agent.onEvent (JiniEvent.PEER_EXIT, endpoint);
        
        liveEndpoints.remove (endpoint);
        deadEndpoints.remove (endpoint);
        failureDetector.remove (endpoint);
        removedEndpoints.put (endpoint, System.currentTimeMillis ());

        agent.disconnectPeer (endpoint);
    }


    private void markAlive (State peerState)
    {
        peerState.setAlive (true);
        liveEndpoints.add (peerState.getEndpoint ());
        deadEndpoints.remove (peerState.getEndpoint ());

        agent.onEvent (JiniEvent.PEER_ALIVE, peerState.getEndpoint (), peerState);
    }

    private void markDead (State peerState)
    {
        peerState.setAlive (false);
        liveEndpoints.remove (peerState.getEndpoint ());
        deadEndpoints.put (peerState.getEndpoint (), System.currentTimeMillis ());
        
        agent.onEvent (JiniEvent.PEER_DEAD, peerState.getEndpoint (), peerState);
    }

    @Override
    public void processCommand (String command, ZMsg msg)
    {

    }

    @Override
    public void processCallback (JiniMsg msg)
    {
        String endpoint = msg.getIdentityString ();
        JiniPeer sender = agent.findPeer (endpoint);
        if (sender == null)
            sender = agent.connectPeer (endpoint);

        if (LOG.isDebugEnabled ()) {
            LOG.debug ("{} ==> {}\n{}", endpoint, agent.getAddress (), msg.dumps ());
        }

        switch (msg.getId ()) {
        case GossipMsg.PING:
            GossipMsg.PingOk pingOk = buildPingOk ((GossipMsg.Ping) msg);
            sender.send (pingOk);
            break;
        case GossipMsg.PING_OK:
            GossipMsg.PingEnd pingEnd = buildPingEnd ((GossipMsg.PingOk) msg);
            sender.send (pingEnd);
            break;
        case GossipMsg.PING_END:
            applyStateChanges (((GossipMsg.PingEnd) msg).getDigest ());
            break;
        case GossipMsg.EXIT:
            removeEndpoint (endpoint);
            break;
        default:
            throw new IllegalStateException ("Illegal Message " + msg.getId ());
        }
    }

    private State getLocalState (String endpoint)
    {
        State localState = null;

        if (endpoint.equals (state.getEndpoint ())) {
            localState = state;
        } else {
            JiniPeer peer = agent.findPeer (endpoint);
            if (peer != null)
                localState = (State) peer.getAttached ();
        }

        return localState;
    }

    private void newPeer (GossipMsg.State remoteState)
    {
        JiniPeer peer = agent.connectPeer (remoteState.getAddressString ());
        agent.onEvent (JiniEvent.PEER_ENTER, peer.getEndpoint ());

        refresh (peer, remoteState);

        State attach = new State (remoteState, peer.getHeaders ());
        peer.attach (attach);

        if (!isPeerManaging (peer))
            markAlive (attach);
        else {
            markDead (attach);
            attach.setManaging (true);
        }

    }

    private void updatePeer (GossipMsg.State remoteState, boolean restart)
    {
        JiniPeer peer = agent.findPeer (remoteState.getAddressString ());
        State peerState = (State) peer.getAttached ();
        if (restart) {
            agent.onEvent (JiniEvent.PEER_RESTART, peer.getEndpoint ());
            failureDetector.clear (remoteState.getAddressString ());
            peer.clearHeaders ();
        }
        refresh (peer, remoteState);

        peerState.setVersion (remoteState.getMaxVersion ());
        peerState.setManaging (isPeerManaging (peer));

        for (Map.Entry <String, VersionedValue> entry : remoteState.getExtra ().entrySet ())
            agent.onEvent (JiniEvent.PEER_HEADER_CHANGE, peer.getEndpoint (),
                            entry.getKey (), entry.getValue ());
    }

    private GossipMsg.PingOk buildPingOk (GossipMsg.Ping msg)
    {
        assert (msg.getDigest ().size () > 0);

        GossipMsg.PingOk pingOk = new GossipMsg.PingOk ();
        for (GossipMsg.State remoteState : msg.getDigest ()) {
            State localState = getLocalState (remoteState.getAddressString ());

            int remoteGeneration = remoteState.getGeneration ();
            int maxRemoteVersion = remoteState.getMaxVersion ();

            if (localState != null) {

                int localGeneration = localState.getGeneration ();
                int maxLocalVersion = localState.getMaxVersion ();

                if (remoteGeneration == localGeneration) {
                    if (maxRemoteVersion > maxLocalVersion) {
                        pingOk.appendRequest (
                                new GossipMsg.State (
                                        remoteState.getAddressString (), remoteGeneration,
                                        maxLocalVersion, null));
                    } else if (maxRemoteVersion < maxLocalVersion) {
                        GossipMsg.State newer = getNewerStateMsg (localState, maxRemoteVersion);
                        if (newer != null)
                            pingOk.appendResponse (newer);
                    }
                } else if (remoteGeneration > localGeneration) {
                    // request all information
                    pingOk.appendRequest (
                            new GossipMsg.State (
                                    remoteState.getAddressString (), remoteGeneration,
                                    0, null));
                } else if (remoteGeneration < localGeneration) {
                    // send all information
                    pingOk.appendResponse (getNewerStateMsg (localState, 0));
                }

            } else {
                //  Unknown Endpoint
                pingOk.appendRequest (
                        new GossipMsg.State (
                                remoteState.getAddressString (), remoteGeneration,
                                0, null));
            }

        }
        return pingOk;
    }

    private GossipMsg.PingEnd buildPingEnd (GossipMsg.PingOk msg)
    {
        GossipMsg.PingEnd pingEnd = new GossipMsg.PingEnd ();
        applyStateChanges (msg.getResponse ());

        for (GossipMsg.State requestState : msg.getRequest ()) {
            State localState = getLocalState (requestState.getAddressString ());
            GossipMsg.State response =
                    getNewerStateMsg (localState, 0);

            if (response != null)
                pingEnd.appendDigest (response);
        }

        return pingEnd;
    }

    private void applyStateChanges (List <GossipMsg.State> statuses)
    {
        for (GossipMsg.State remoteState : statuses) {
            String remoteEndpoint = remoteState.getAddressString ();
            State localState = getLocalState (remoteEndpoint);

            if (remoteEndpoint.equals (state.getEndpoint ()))
                continue;

            if (removedEndpoints.containsKey (remoteEndpoint))
                continue;

            if (localState != null) {

                int localGeneration = localState.getGeneration ();
                int remoteGeneration = remoteState.getGeneration ();

                if (remoteGeneration > localGeneration) {
                    updatePeer (remoteState, true);
                } else if (remoteGeneration == localGeneration) {
                    int localMaxVersion = localState.getMaxVersion ();
                    int remoteMaxVersion = remoteState.getMaxVersion ();

                    if (remoteMaxVersion > localMaxVersion) {
                        updatePeer (remoteState, false);
                    }

                    if (!localState.isAlive () && !localState.isManaging ())
                        markAlive (localState);
                }
            } else {
                newPeer (remoteState);
            }
        }
    }

    private GossipMsg.State getNewerStateMsg (State localState, int version)
    {
        assert (localState != null);

        GossipMsg.State result = null;

        if (localState.getMaxVersion () > version)
            result = localState.getMessage ();

        for (Map.Entry <String, VersionedValue> entry : localState.getHeaders ().entrySet ())
        {
            VersionedValue value = entry.getValue ();
            if (value.getVersion () > version) {
                result.getExtra ().put (entry.getKey (), entry.getValue ());
            }
        }

        return result;
    }

    private void refresh (JiniPeer peer, GossipMsg.State remoteState)
    {
        if (remoteState != null)
            peer.updateHeaders (remoteState.getExtra ());
        peer.refresh (evasive, expired);
        failureDetector.report (peer.getEndpoint ());
    }

    @Override
    public void onEvent (JiniEvent event, Object ... params)
    {

    }

    @Override
    public void destroy ()
    {
        GossipMsg.Exit msg = new GossipMsg.Exit ();
        for (String endpoint : liveEndpoints)
            agent.findPeer (endpoint).send (msg);
    }

}
