/*
 =========================================================================
     TestGossipDiscovery.java

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

package org.jeromq.jini;

import static org.junit.Assert.*;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.jeromq.ZMsg;
import org.junit.Test;

public class TestGossipDiscovery 
{
    static class App extends Thread 
    {
        private final List <String> peers;
        private final Properties conf;
        private Jini jini;
        
        App (String bind, String endpoints)
        {
            conf = new Properties ();
            conf.setProperty ("bind", bind);  // agent bind
            conf.setProperty ("heartbeat.strategy", "gossip");
            conf.setProperty ("heartbeat.endpoints", endpoints); // seeds
            conf.setProperty ("heartbeat.args.0", String.valueOf (System.currentTimeMillis () / 1000)); // generation
            
            peers = new ArrayList <String> ();
        }
        
        @Override
        public void run ()
        {
            jini = new Jini (1, conf);
            jini.registerCommandCallback ("*", new IJiniHandler () {

                @Override
                public void processCommandCallback (Jini jini, String command,
                        ZMsg msg)
                {
                    StringWriter w = new StringWriter ();
                    w.write (String.format ("[%s] COMMAND: %s\n", conf.getProperty ("bind"), command));
                    msg.dump (w);
                    w.write ("=========================\n");
                    
                    System.out.print (w);
                    
                    if (command.equals ("PEER.ENTER")) {
                        peers.add (msg.popString ());
                    }
                }});
            jini.loop ();
        }

        public void shutdown ()
        {
            jini.destory ();
        }
        
        public List <String> getPeers ()
        {
            return peers;
        }
        
        public List <String> getPeerEndpoints ()
        {
            Collections.sort (peers);
            
            return peers;
        }
    }
    @Test
    public void testDiscovery () throws Exception
    {
        App app1 = new App ("127.0.0.1:5555", "127.0.0.1:5556");
        App app2 = new App ("127.0.0.1:5556", "127.0.0.1:5556");
        App app3 = new App ("127.0.0.1:5557", "127.0.0.1:5556");

        app1.start ();
        app2.start ();
        app3.start ();
        
        Thread.sleep (3000);
        
        app1.shutdown ();
        app2.shutdown ();
        app3.shutdown ();
        
        app1.join ();
        app2.join ();
        app3.join ();
        
        List <String> endpoints1 = app1.getPeerEndpoints ();
        List <String> endpoints2 = app2.getPeerEndpoints ();
        List <String> endpoints3 = app3.getPeerEndpoints ();

        assertEquals (2, endpoints1.size ());
        assertEquals ("127.0.0.1:5556", endpoints1.get (0));
        assertEquals ("127.0.0.1:5557", endpoints1.get (1));

        assertEquals (2, endpoints2.size ());
        assertEquals ("127.0.0.1:5555", endpoints2.get (0));
        assertEquals ("127.0.0.1:5557", endpoints2.get (1));

        assertEquals (2, endpoints3.size ());
        assertEquals ("127.0.0.1:5555", endpoints3.get (0));
        assertEquals ("127.0.0.1:5556", endpoints3.get (1));

    }
}
