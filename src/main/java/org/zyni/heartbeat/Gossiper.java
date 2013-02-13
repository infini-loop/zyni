package org.zyni.heartbeat;

import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMsg;
import org.zyni.IAgentHandler;
import org.zyni.ZyniAgent;
import org.zyni.ZyniEvent;
import org.zyni.ZyniMsg;
import org.zyni.ZyniMsg.VersionedMap;
import org.zyni.ZyniPeer;
import org.zyni.message.Common;
import org.zyni.message.Common.VersionedValue;
import org.zyni.message.Gossip;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;


public class Gossiper implements IAgentHandler
{
    private final static Logger LOG = LoggerFactory.getLogger (Gossiper.class);

    private ZyniAgent agent;

    private final String hosts;
    private final Set<String> seeds;
    private final Set <String> liveEndpoints;
    private final Map<String, Long> deadEndpoints;
    private final Map <String, Long> removedEndpoints;
    private final int generation;
    private final long evasive;
    private final long expired;
    private final Random rand;
    private List<String> managingStatus;
    private long interval;
    private FailureDetector failureDetector;

    private State state;
    private int msgId;

    private static class State {

        private final int generation;
        private int version;
        private final String endpoint;
        private boolean alive;
        private boolean managing;
        private final VersionedMap headers;

        public State (Gossip.State msg, VersionedMap headers)
        {
            this (ZyniMsg.addressToString (msg.getAddress ()), msg.getGeneration (),
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

        public Gossip.State getMessage ()
        {
            Common.Address address = ZyniMsg.stringToAddress (endpoint);
            Gossip.State state = Gossip.State.newBuilder ()
                                    .setAddress (address)
                                    .setMaxVersion (getMaxVersion ())
                                    .setGeneration (getGeneration ())
                                    .build ();

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

        seeds = new HashSet<String> ();
        liveEndpoints = new HashSet <String> ();
        deadEndpoints = new HashMap<String, Long> ();
        removedEndpoints = new HashMap <String, Long> ();
        rand = new Random (System.currentTimeMillis ());

        generation = Integer.parseInt (args[0]);
    }

    @Override
    public int getSignature ()
    {
        int signature = Gossip.getDescriptor ().hashCode ();

        LOG.info ("Gossip Signature {}", signature);
        ZyniMsg.registerBuilder (signature, Gossip.Ping.newBuilder ());
        ZyniMsg.registerBuilder (signature, Gossip.PingAck.newBuilder ());
        ZyniMsg.registerBuilder (signature, Gossip.PingEnd.newBuilder ());
        ZyniMsg.registerBuilder (signature, Gossip.Exit.newBuilder ());

        return signature;
    }

    @Override
    public void initialize (ZyniAgent agent, Object ... args)
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

        Map <String, ZyniPeer> peers = agent.getPeers ();
        if (peers.size () > 0)
        {
            Gossip.Ping.Builder builder = Gossip.Ping.newBuilder ();
            builder.addDigest (state.getMessage ());

            for (ZyniPeer peer : peers.values ()) {
                State peerState = (State) peer.getAttached ();
                if (peerState != null)
                    builder.addDigest (peerState.getMessage ());
            }

            Gossip.Ping msg = builder.build ();
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

    private void sendGossipDead (Gossip.Ping msg)
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

    private boolean sendGossip (Gossip.Ping msg, Set <String> peers)
    {
        int size = peers.size ();
        if (size == 0)
            return false;

        int idx = rand.nextInt (size);
        for (String endpoint: peers) {
            if (idx-- == 0) {
                if (LOG.isDebugEnabled ())
                    LOG.debug ("{} sends gossip to {}", state.getEndpoint (), endpoint);
                send (agent.findPeer (endpoint), msg);
                return seeds.contains (endpoint);
            }
        }
        return false;
    }

    private void checkState ()
    {
        long now = System.currentTimeMillis();

        Map <String, ZyniPeer> peers = agent.getPeers ();
        for (Map.Entry <String, ZyniPeer> entry : peers.entrySet ())
        {
            String endpoint = entry.getKey ();
            ZyniPeer peer = entry.getValue ();
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

        Iterator<Entry <String, Long>> it = removedEndpoints.entrySet ().iterator ();
        while (it.hasNext ()) {
            Map.Entry <String, Long> entry = it.next ();
            if (now - entry.getValue () > evasive * 2) {
                agent.removePeer (entry.getKey ());
                it.remove ();
            }
        }

    }

    private boolean isPeerManaging (ZyniPeer peer)
    {
        String value = peer.getHeaders ().getValue ("STATUS");
        if (value == null)
            return false;

        String status = value.split (",") [0];

        return managingStatus.contains (status);
    }

    private void removeEndpoint (String endpoint)
    {
        agent.onEvent (ZyniEvent.PEER_EXIT, endpoint);

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

        agent.onEvent (ZyniEvent.PEER_ALIVE, peerState.getEndpoint (), peerState);
    }

    private void markDead (State peerState)
    {
        peerState.setAlive (false);
        liveEndpoints.remove (peerState.getEndpoint ());
        deadEndpoints.put (peerState.getEndpoint (), System.currentTimeMillis ());

        agent.onEvent (ZyniEvent.PEER_DEAD, peerState.getEndpoint (), peerState);
    }

    @Override
    public void processCommand (String command, ZMsg msg)
    {

    }

    @Override
    public void processCallback (ZyniMsg msg)
    {
        String endpoint = msg.getIdentityString ();
        ZyniPeer sender = agent.findPeer (endpoint);
        if (sender == null)
            sender = agent.connectPeer (endpoint);

        if (LOG.isDebugEnabled ()) {
            LOG.debug ("{} ==> {}\n{}", endpoint, agent.getAddress (), msg.dumps ());
        }

        Message body = msg.getBody ();

        if (body instanceof Gossip.Ping) {
            Gossip.PingAck pingAck = buildPingOk ((Gossip.Ping) body);
            send (sender, pingAck);
        } else if (body instanceof Gossip.PingAck) {
            Gossip.PingEnd pingEnd = buildPingEnd ((Gossip.PingAck) body);
            send (sender, pingEnd);
        } else if (body instanceof Gossip.PingEnd) {
            applyStateChanges (((Gossip.PingEnd) body).getDigestList ());
        } else if (body instanceof Gossip.Exit) {
            removeEndpoint (endpoint);
        } else {
            throw new IllegalStateException ("Illegal Message " + msg.getType ());
        }
    }

    private State getLocalState (Gossip.State remoteState)
    {
        String endpoint = ZyniMsg.addressToString (remoteState.getAddress ());
        State localState = null;

        if (endpoint.equals (state.getEndpoint ())) {
            localState = state;
        } else {
            ZyniPeer peer = agent.findPeer (endpoint);
            if (peer != null)
                localState = (State) peer.getAttached ();
        }

        return localState;
    }

    private void newPeer (Gossip.State remoteState)
    {
        ZyniPeer peer = agent.connectPeer (ZyniMsg.addressToString (remoteState.getAddress ()));
        agent.onEvent (ZyniEvent.PEER_ENTER, peer.getEndpoint ());

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

    private void updatePeer (Gossip.State remoteState, boolean restart)
    {
        ZyniPeer peer = agent.findPeer (ZyniMsg.addressToString (remoteState.getAddress ()));
        State peerState = (State) peer.getAttached ();
        if (restart) {
            agent.onEvent (ZyniEvent.PEER_RESTART, peer.getEndpoint ());
            failureDetector.clear (ZyniMsg.addressToString (remoteState.getAddress ()));
            peer.clearHeaders ();
        }
        refresh (peer, remoteState);

        peerState.setVersion (remoteState.getMaxVersion ());
        peerState.setManaging (isPeerManaging (peer));

        for (VersionedValue entry : remoteState.getExtraList ())
            agent.onEvent (ZyniEvent.PEER_HEADER, peer.getEndpoint (),
                    entry.getKey (), entry.getValue ());
    }

    private Gossip.PingAck buildPingOk (Gossip.Ping msg)
    {
        assert (msg.getDigestList ().size () > 0);

        Gossip.PingAck.Builder builder = Gossip.PingAck.newBuilder ();
        for (Gossip.State remoteState : msg.getDigestList ()) {

            State localState = getLocalState (remoteState);

            int remoteGeneration = remoteState.getGeneration ();
            int maxRemoteVersion = remoteState.getMaxVersion ();

            if (localState != null) {

                int localGeneration = localState.getGeneration ();
                int maxLocalVersion = localState.getMaxVersion ();

                if (remoteGeneration == localGeneration) {
                    if (maxRemoteVersion > maxLocalVersion) {
                        builder.addRequest (
                                Gossip.State.newBuilder ()
                                        .setAddress (remoteState.getAddress ())
                                        .setGeneration (remoteGeneration)
                                        .setMaxVersion (maxLocalVersion)
                                        .build ());
                    } else if (maxRemoteVersion < maxLocalVersion) {
                        Gossip.State newer = getNewerStateMsg (localState, maxRemoteVersion);
                        if (newer != null)
                            builder.addResponse (newer);
                    }
                } else if (remoteGeneration > localGeneration) {
                    // request all information
                    builder.addRequest (
                            Gossip.State.newBuilder ()
                                    .setAddress (remoteState.getAddress ())
                                    .setGeneration (remoteGeneration)
                                    .setMaxVersion (0)
                                    .build ());
                } else if (remoteGeneration < localGeneration) {
                    // send all information
                    builder.addResponse (getNewerStateMsg (localState, 0));
                }

            } else {
                //  Unknown Endpoint
                builder.addRequest (
                        Gossip.State.newBuilder ()
                                .setAddress (remoteState.getAddress ())
                                .setGeneration (remoteGeneration)
                                .setMaxVersion (0)
                                .build ());
            }

        }
        return builder.build ();
    }

    private Gossip.PingEnd buildPingEnd (Gossip.PingAck msg)
    {
        Gossip.PingEnd.Builder builder = Gossip.PingEnd.newBuilder ();
        applyStateChanges (msg.getResponseList ());

        for (Gossip.State requestState : msg.getRequestList ()) {
            State localState = getLocalState (requestState);
            Gossip.State response =
                    getNewerStateMsg (localState, 0);

            if (response != null)
                builder.addDigest (response);
        }

        return builder.build ();
    }

    private void applyStateChanges (List <Gossip.State> statuses)
    {
        for (Gossip.State remoteState : statuses) {
            State localState = getLocalState (remoteState);
            String remoteEndpoint = ZyniMsg.addressToString (remoteState.getAddress ());

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

    private Gossip.State getNewerStateMsg (State localState, int version)
    {
        assert (localState != null);

        Gossip.State.Builder builder = Gossip.State.newBuilder ();

        if (localState.getMaxVersion () > version)
            builder.mergeFrom (localState.getMessage ());

        for (Map.Entry <String, VersionedValue> entry : localState.getHeaders ().entrySet ())
        {
            VersionedValue value = entry.getValue ();
            if (value.getVersion () > version) {
                builder.addExtra (value);
            }
        }

        return builder.build ();
    }

    private void refresh (ZyniPeer peer, Gossip.State remoteState)
    {
        if (remoteState != null)
            peer.updateHeaders (remoteState.getExtraList ());
        peer.refresh (evasive, expired);
        failureDetector.report (peer.getEndpoint ());
    }

    private void send (ZyniPeer peer, Message msg)
    {
        peer.send (new ZyniMsg (msg));
    }

    @Override
    public void onEvent (ZyniEvent event, Object ... params)
    {

    }

    @Override
    public void destroy ()
    {
        Gossip.Exit msg = Gossip.Exit.newBuilder ().build ();
        for (String endpoint : liveEndpoints)
            send (agent.findPeer (endpoint), msg);
    }
}
