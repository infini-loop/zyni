package org.zyni;

import org.zeromq.ZMsg;

public interface IAgentHandler
{
    /**
     * Initialize handler, save the instance into a private variable
     * @param agent JiniAgent instance
     * @param args optional argument
     */
    void initialize (ZyniAgent agent, Object ... args);

    /**
     *
     * register message signature of handler
     */
    int getSignature ();

    /**
     * Handles command from API
     * @param command
     * @param msg ZMsg instance. Command is not included
     */
    void processCommand (String command, ZMsg msg);

    /**
     * Handles command from other Handler before poping up to API
     * @param event
     * @param params
     */
    void onEvent (ZyniEvent event, Object ... params);

    /**
     * Handles message having handler's sequence
     * @param msg instance of generated JiniMsg subclass
     */
    void processCallback (ZyniMsg msg);

    /**
     *
     * @param interval previous interval
     * @return next interval. If it is less than 0, timer is stopped
     */
    long processTimer (long interval);

    void destroy ();
}
