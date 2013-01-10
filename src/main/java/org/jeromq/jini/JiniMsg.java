/*
 =========================================================================
     JiniMsg.java

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

import java.util.Collection;
import java.util.Map;
import java.util.HashMap;
import java.nio.ByteBuffer;
import java.util.Set;

import org.jeromq.ZFrame;
import org.jeromq.ZMQ;
import org.jeromq.ZMQ.Socket;
import org.jeromq.ZMsg;

//  Opaque class structure
public class JiniMsg 
{
    public interface Builder
    {
        JiniMsg build (JiniMsg self, Socket input);
    }

    private static final Map <Integer, Builder> builders 
                    = new HashMap <Integer, Builder> ();
    
    //  Structure of our class
    private ZFrame identity;           //  Address of peer if any
    private int id;                     //  Jini message ID
    private ByteBuffer needle;          //  Read/write pointer for serialization
    private int signature;              //  Signature of service
    private int version;                //  Protocol version
    
    private static Builder getBuilder (int signature)
    {
        return builders.get (signature);
    }
    
    public static void registerBuilder (int signature, Builder builder)
    {
        builders.put (signature, builder);
    }
    
    //  --------------------------------------------------------------------------
    //  Create a new JiniMsg

    protected JiniMsg (int id, int signature)
    {
        this.id = id;
        this.signature = signature;
    }

    protected JiniMsg (JiniMsg parent)
    {
        this.version = parent.version;
        this.id = parent.id;
        this.identity = parent.identity;
        this.needle = parent.needle;
        this.signature = parent.signature;
    }

    //  --------------------------------------------------------------------------
    //  Destroy the JiniMsg

    protected void destroy ()
    {
        //  Free class properties
        if (identity != null)
            identity.destroy ();
        identity = null;
    }


    //  --------------------------------------------------------------------------
    //  Network data encoding macros

    protected int unsigned (int value)
    {
        if (value < 0)
            value = (0xffff) & value;
        return value;
    }
    
    protected long unsignedLong (int value)
    {
        if (value < 0)
            value = (0xffffffff) & value;
        return value;
    }

    //  Put a 1-byte number to the frame
    protected void putNumber1 (int value) 
    {
        needle.put ((byte) value);
    }

    //  Get a 1-byte number to the frame
    //  then make it unsigned
    protected int getNumber1 () 
    { 
        int value = needle.get (); 
        return value;
    }

    //  Put a 2-byte number to the frame
    protected void putNumber2 (int value) 
    {
        needle.putShort ((short) value);
    }

    //  Get a 2-byte number to the frame
    protected int getNumber2 () 
    { 
        return needle.getShort ();
    }

    //  Put a 4-byte number to the frame
    protected void putNumber4 (int value) 
    {
        needle.putInt (value);
    }

    //  Get a 4-byte number to the frame
    protected int getNumber4 () 
    { 
        return needle.getInt (); 
    }

    //  Put a 8-byte number to the frame
    protected void putNumber8 (long value) 
    {
        needle.putLong (value);
    }

    //  Get a 8-byte number to the frame
    protected long getNumber8 () 
    {
        return needle.getLong ();
    }


    //  Put a block to the frame
    protected void putBlock (byte [] value, int size) 
    {
        needle.put (value, 0, size);
    }

    protected byte [] getBlock (int size) 
    {
        byte [] value = new byte [size]; 
        needle.get (value);

        return value;
    }

    //  Put a string to the frame
    protected void putString (String value) 
    {
        needle.put ((byte) value.length ());
        needle.put (value.getBytes());
    }

    //  Get a string from the frame
    protected String getString () 
    {
        int size = getNumber1 ();
        byte [] value = new byte [size];
        needle.get (value);

        return new String (value);
    }

    //  Get the needle
    protected ByteBuffer getNeedle ()
    {
        return needle;
    }

    //  Set the needle
    public ByteBuffer setNeedle (ByteBuffer buf)
    {
        return needle = buf;
    }

    //  --------------------------------------------------------------------------
    //  Receive and parse a JiniMsg from the socket. Returns new object or
    //  null if error. Will block if there's no message waiting.

    public static JiniMsg recv (Socket input)
    {
        assert (input != null);
        JiniMsg self = new JiniMsg (0, 0);
        ZFrame frame = null;

        try {
            //  If we're reading from a ROUTER socket, get address
            if (input.getType () == ZMQ.ROUTER) {
                self.identity = ZFrame.recvFrame (input);
                if (self.identity == null)
                    return null;         //  Interrupted
                if (!input.hasReceiveMore ())
                    throw new IllegalArgumentException ();
            }
            //  Read and parse command in frame
            frame = ZFrame.recvFrame (input);
            if (frame == null)
                return null;             //  Interrupted

            //  Get and check protocol signature
            self.needle = ByteBuffer.wrap (frame.getData ()); 
            self.signature = self.getNumber4 ();
            self.version = self.getNumber1 ();

            //  Get message id, which is first byte in frame
            self.id = self.unsigned (self.getNumber2 ());

            Builder builder = getBuilder (self.signature);
            if (builder == null) {
                //  Protocol assertion, drop message
                while (input.hasReceiveMore ()) {
                    frame.destroy ();
                    frame = ZFrame.recvFrame (input);
                }
            }
            
            JiniMsg msg = builder.build (self, input);
            
            return msg;

        } catch (Exception e) {
            //  Error returns
            System.out.printf ("E: malformed message '%d'\n", self.id);
            self.destroy ();
            return null;
        } finally {
            if (frame != null)
                frame.destroy ();
        }
    }

    //  --------------------------------------------------------------------------
    //  Send the JiniMsg to the socket, and destroy it

    public boolean send (Socket socket)
    {
        assert (socket != null);
        boolean ret = true;
        //  Calculate size of serialized data
        int frameSize = 4 + 1 + 2;          //  Signature, Version, and message ID
        
        frameSize += getFrameSize ();
        //  Now serialize message into the frame
        ZFrame frame = new ZFrame (new byte [frameSize]);
        needle = ByteBuffer.wrap (frame.getData ()); 
        putNumber4 (signature);
        putNumber1 (version);
        putNumber2 (id);

        ZMsg msg = serialize ();
        
        //  If we're sending to a ROUTER, we send the address first
        if (socket.getType () == ZMQ.ROUTER) {
            assert (identity != null);
            if (!identity.sendAndDestroy (socket, ZFrame.MORE)) {
                destroy ();
                return false;
            }
        }
        //  Now send the data frame
        if (!frame.sendAndDestroy (socket, msg != null ? ZFrame.MORE : 0)) {
            frame.destroy ();
            destroy ();
            return false;
        }
        
        if (msg != null)
            ret = msg.send (socket);
        
        //  Destroy JiniMsg object
        destroy ();
        return ret;
    }

    protected int getFrameSize ()
    {
        throw new RuntimeException ("Must override this method");
    }
    
    protected ZMsg serialize ()
    {
        throw new RuntimeException ("Must override this method");
    }

    public void dump ()
    {
        throw new RuntimeException ("Must override this method");
    }

    public String dumps ()
    {
        throw new RuntimeException ("Must override this method");
    }

    //  --------------------------------------------------------------------------
    //  Get/set the message address

    public ZFrame getIdentity ()
    {
        return identity;
    }
    
    public String getIdentityString ()
    {
        return new String (identity.getData ());
    }

    public void setAddress (ZFrame address)
    {
        if (this.identity != null)
            this.identity.destroy ();
        this.identity = address.duplicate ();
    }


    //  --------------------------------------------------------------------------
    //  Get/set the JiniMsg id

    public int getId ()
    {
        return id;
    }

    public void setId (int id)
    {
        this.id = id;
    }

    //  --------------------------------------------------------------------------
    //  Get/set the JiniMsg signature

    public int getSignature ()
    {
        return signature;
    }

    public static class VersionedValue
    {
        private int version;
        private String value;
        
        public VersionedValue (int version, String value)
        {
            this.version = version;
            this.value = value;
        }
        
        public int getVersion () 
        {
            return version;
        }
        
        public String getValue ()
        {
            return value;
        }

        public void setValue (String value)
        {
            version++;
            this.value = value;
        }
    }

    public static class VersionedMap implements Map <String, VersionedValue> {

        private Map <String, VersionedValue> data;
        private int maxVersion;

        public VersionedMap ()
        {
            maxVersion = 0;
            data = new HashMap<String, VersionedValue> ();
        }

        public VersionedMap (VersionedMap old)
        {
            maxVersion = old.maxVersion;
            data = new HashMap<String, VersionedValue> (old.data);
        }

        public VersionedValue put (String key, int version, String value)
        {
            return put (key, new VersionedValue (version, value));
        }

        public VersionedValue put (String key, String value)
        {
            return put (key, 0, value);
        }

        public int getMaxVersion ()
        {
            return maxVersion;
        }

        @Override
        public int size ()
        {
            return data.size ();
        }

        @Override
        public boolean isEmpty ()
        {
            return data.isEmpty ();
        }

        @Override
        public boolean containsKey (Object key)
        {
            return data.containsKey (key);
        }

        @Override
        public boolean containsValue (Object value)
        {
            //  TODO: implement this precisely
            return containsValue (value);
        }

        @Override
        public VersionedValue get (Object key)
        {
            return data.get (key);
        }

        @Override
        public VersionedValue put (String key, VersionedValue value)
        {
            if (value.getVersion () > maxVersion)
                maxVersion = value.getVersion ();

            return data.put (key, value);
        }

        @Override
        public VersionedValue remove (Object key)
        {
            return data.remove (key);
        }

        @Override
        public void putAll (Map<? extends String, ? extends VersionedValue> m)
        {
            data.putAll (m);
        }

        @Override
        public void clear ()
        {
            data.clear ();
            maxVersion = 0;
        }

        @Override
        public Set<String> keySet ()
        {
            return data.keySet ();
        }

        @Override
        public Collection<VersionedValue> values ()
        {
            return data.values ();
        }

        @Override
        public Set<Entry<String, VersionedValue>> entrySet ()
        {
            return data.entrySet ();
        }

        public String getValue (String key)
        {
            VersionedValue vvalue = get (key);
            if (vvalue != null)
                return vvalue.getValue ();
            return null;
        }
    }
}

