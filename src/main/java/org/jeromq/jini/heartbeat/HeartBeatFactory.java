/*
 =========================================================================
     HeartBeatFactory.java

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

import org.jeromq.jini.IAgentHandler;

public class HeartBeatFactory
{

    public static IAgentHandler getInstance (String type, String hosts, 
                                            long evasive, long expired, 
                                            String ... args)
    {
        if (type.equals ("gossip"))
            return new Gossiper (hosts, evasive, expired, args);

        if (type.equals ("all"))
            return new HeartBeater (hosts, evasive, expired, args);

        return null;
    }

}
