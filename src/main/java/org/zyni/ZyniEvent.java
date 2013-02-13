package org.zyni;

public enum ZyniEvent
{
    SYSTEM_EXIT ("SYSTEM.EXIT"),

    AGENT_EXIT ("AGENT.EXIT"),
    AGENT_HEADER ("AGENT.HEADER"),

    PEER_ENTER ("PEER.ENTER"),
    PEER_EXIT ("PEER.EXIT"),
    PEER_DEAD ("PEER.DEAD"),
    PEER_HEADER ("PEER.HEADER"),
    PEER_RESTART ("PEER.RESTART"),
    PEER_ALIVE ("PEER.ALIVE");

    private String command;

    private ZyniEvent (String command)
    {
        this.command = command;
    }

    @Override
    public String toString ()
    {
        return command;
    }
}
