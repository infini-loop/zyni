/*
 =========================================================================
     JiniPeer.java

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

import java.util.Map;

import org.jeromq.ZContext;
import org.jeromq.ZMQ;
import org.jeromq.ZMQ.Socket;
import org.jeromq.jini.JiniMsg.VersionedMap;

public class JiniPeer
{

    private final ZContext ctx;          //  ZMQ context
    private Socket mailbox;              //  Socket through to peer
    private final String endpoint;       //  Endpoint connected to
    private long evasiveAt;              //  Peer is being evasive
    private long expiredAt;              //  Peer has expired by now
    private boolean connected;           //  Peer will send messages
    private boolean ready;               //  Peer has sent an ack to us
    private int sequence;                //  Message sequence
    
    private final VersionedMap headers;
    private Object attached;
    
    private JiniPeer (ZContext ctx, String endpoint)
    {
        this.ctx = ctx;
        this.endpoint = endpoint;
        
        connected = false;
        sequence = 0;
        
        headers = new VersionedMap ();
    }
    
    //  ---------------------------------------------------------------------
    //  Construct new peer object
    public static JiniPeer create (ZContext ctx, String endpoint, Map <String, JiniPeer> container)
    {
        JiniPeer peer = new JiniPeer (ctx, endpoint);
        
        if (container != null)
            container.put (endpoint, peer);

        return peer;
    }
    
    //  ---------------------------------------------------------------------
    //  Connect peer mailbox
    //  Configures mailbox and connects to peer's router endpoint
    public void connect (String replyTo, int hwm)
    {
        if (connected)
            return;

        //  Create new outgoing socket (drop any messages in transit)
        mailbox = ctx.createSocket (ZMQ.DEALER);

        //  Null if shutting down
        if (mailbox != null) {
            //  Set our caller 'From' identity so that receiving node knows
            //  who each message came from.
            mailbox.setIdentity (replyTo.getBytes ());

            //  Set a high-water mark that allows for reasonable activity
            mailbox.setSndHWM (hwm);

            //  Send messages immediately or return EAGAIN
            mailbox.setSendTimeOut (0);

            //  Connect through to peer node
            mailbox.connect (String.format ("tcp://%s", endpoint));
            connected = true;
            ready = false;
        }
    }

    public String getEndpoint ()
    {
        return endpoint;
    }

    public Socket getHandle ()
    {
        return mailbox;
    }

    public void refresh (long evasive, long expired)
    {
        long now = System.currentTimeMillis ();
        evasiveAt = now + evasive;
        expiredAt = now + expired;
    }

    public long expiredAt ()
    {
        return expiredAt;
    }

    public long evasiveAt ()
    {
        return evasiveAt;
    }

    public void destory ()
    {
        disconnect ();
    }

    public boolean send (JiniMsg msg)
    {
        return msg.send (mailbox);
    }

    public boolean getReady ()
    {
        return ready;
    }

    public void setReady (boolean ready)
    {
        this.ready = ready;
    }

    public Object getAttached ()
    {
        return attached;
    }
    
    public int incSequence ()
    {
        return ++sequence;
    }

    public VersionedMap getHeaders ()
    {
        return headers;
    }

    public boolean getSpecial ()
    {
        return false;
    }

    public void attach (Object attached)
    {
        this.attached = attached;
    }

    public void clearHeaders ()
    {
        headers.clear ();
    }

    public void updateHeaders (VersionedMap headers)
    {
        this.headers.putAll (headers);
    }


    public void disconnect ()
    {
        if (connected)
            ctx.destroySocket (mailbox);
        mailbox = null;
        connected = false;
    }
}
