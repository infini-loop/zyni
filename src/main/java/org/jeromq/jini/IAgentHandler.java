/*
 =========================================================================
     IAgentHandler.java

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

     * @param args optional arguments
     You should have , Object ... argsreceived a copy of the GNU Lesser General Public License
     along with this program. If not, see http://www.gnu.org/licenses/.
     =========================================================================
 */

package org.jeromq.jini;

import org.jeromq.ZMsg;

public interface IAgentHandler
{
    /**
     * Initialize handler, save the instance into a private variable
     * @param agent JiniAgent instance
     * @param args optional argument
     */
    void initialize (JiniAgent agent, Object ... args);

    /**
     * 
     * @return message signature of handler
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
    void onEvent (JiniEvent event, Object ... params);

    /**
     * Handles message having handler's sequence
     * @param msg instance of generated JiniMsg subclass
     */
    void processCallback (JiniMsg msg);

    /**
     * 
     * @param interval previous interval
     * @return next interval. If it is less than 0, timer is stopped
     */
    long processTimer (long interval);

    void destroy ();
}
