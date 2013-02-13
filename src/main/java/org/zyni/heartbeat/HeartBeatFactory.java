package org.zyni.heartbeat;

import org.zyni.IAgentHandler;

public class HeartBeatFactory
{
    public static IAgentHandler getInstance (String type, String hosts,
                                             long evasive, long expired,
                                             String ... args)
    {
        if (type.equals ("gossip"))
            return new Gossiper (hosts, evasive, expired, args);

        return null;
    }
}
