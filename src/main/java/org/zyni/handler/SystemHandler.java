package org.zyni.handler;

import org.zeromq.ZMsg;
import org.zyni.IAgentHandler;
import org.zyni.ZyniAgent;
import org.zyni.ZyniEvent;
import org.zyni.ZyniMsg;

public class SystemHandler implements IAgentHandler
{
    private ZyniAgent agent;

    @Override
    public void initialize (ZyniAgent agent, Object ... args)
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
        if (command.equals (ZyniEvent.AGENT_HEADER.toString ())) {
            agent.putHeader (msg.popString (), msg.popString ());
        }
        else if (command.equals (ZyniEvent.SYSTEM_EXIT.toString ())) {
            agent.stop ();
        }
    }

    @Override
    public void onEvent (ZyniEvent event, Object ... params)
    {
        switch (event) {
        case PEER_ENTER:
            agent.sendCommand (event.toString (), (String) params [0]);
            break;
        case PEER_EXIT:
            agent.sendCommand (event.toString (), (String) params [0]);
            break;
        default:
            break;
        }

    }

    @Override
    public void processCallback (ZyniMsg msg)
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

