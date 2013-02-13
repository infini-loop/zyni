package org.zyni;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zyni.message.Common.Address;
import org.zyni.message.Common.Header;
import org.zyni.message.Common.VersionedValue;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ZyniMsg
{
    private static Logger LOG = LoggerFactory.getLogger (ZyniMsg.class);

    private static final Map<Integer, Message.Builder> builders
            = new HashMap<Integer, Message.Builder> ();
    private static final Map<Integer, Integer> builderSignatures
            = new HashMap<Integer, Integer> ();

    //  Structure of our class
    private ZFrame identity;           //  Address of peer if any
    private Header header;             //  Message Headeer
    private Message body;              //  Message Body

    private static Message.Builder getBuilder (int type)
    {
        Message.Builder builder = builders.get (type);
        if (builder != null)
            builder = builder.clone ();

        return builder;
    }

    public static void registerBuilder (int signature, Message.Builder builder)
    {
        int type = builder.getDescriptorForType ().hashCode ();
        builders.put (type, builder);
        builderSignatures.put (type, signature);
    }

    //  --------------------------------------------------------------------------
    //  Create a new ZyniMsg

    public ZyniMsg (Message body)
    {
        int type = body.getDescriptorForType ().hashCode ();
        this.header = Header.newBuilder ()
                        .setType (type)
                        .setSignature (builderSignatures.get (type))
                        .build ();
        this.body = body;
    }

    protected ZyniMsg (ZFrame identity, Header header, Message body)
    {
        this.identity = identity;
        this.header = header;
        this.body = body;
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
    //  Receive and parse a ZyniMsg from the socket. Returns new object or
    //  null if error. Will block if there's no message waiting.

    public static ZyniMsg recv (Socket input)
    {
        assert (input != null);
        ZFrame identity = null;
        ZFrame frame = null;

        try {
            //  If we're reading from a ROUTER socket, get address
            if (input.getType () == ZMQ.ROUTER) {
                identity = ZFrame.recvFrame (input);
                if (identity == null)
                    return null;         //  Interrupted

                if (!input.hasReceiveMore ())
                    throw new IllegalArgumentException ();
            }
            //  Read and parse command in frame
            frame = ZFrame.recvFrame (input);
            if (frame == null)
                return null;             //  Interrupted

            if (!input.hasReceiveMore ())
                throw new IllegalArgumentException ();

            //  Get and check protocol signature
            Header header = Header.parseFrom (frame.getData ());

            Message.Builder builder = getBuilder (header.getType ());
            if (builder == null) {
                //  Protocol assertion, drop message
                while (input.hasReceiveMore ()) {
                    frame.destroy ();
                    frame = ZFrame.recvFrame (input);
                }
            }

            frame = ZFrame.recvFrame (input);
            if (frame == null)
                return null;             //  Interrupted

            if (input.hasReceiveMore ())   //  Must be last part
                throw new IllegalArgumentException ();

            Message body = builder.mergeFrom (frame.getData ()).build ();

            return new ZyniMsg (identity, header, body);

        } catch (Exception e) {
            LOG.error ("malformed message", e);
            return null;
        }
    }

    //  --------------------------------------------------------------------------
    //  Send the JiniMsg to the socket, and destroy it

    public boolean send (Socket socket)
    {
        assert (socket != null);
        boolean ret = true;
        //  Calculate size of serialized data

        //  If we're sending to a ROUTER, we send the address first
        if (socket.getType () == ZMQ.ROUTER) {
            assert (identity != null);
            if (!identity.sendAndDestroy (socket, ZFrame.MORE)) {
                destroy ();
                return false;
            }
        }

        //  Now send the data frame
        if (!socket.send (header.toByteArray (), ZMQ.SNDMORE)) {
            destroy ();
            return false;
        }

        //  Now send the data frame
        if (!socket.send (body.toByteArray (), 0)) {
            destroy ();
            return false;
        }

        //  Destroy JiniMsg object
        destroy ();
        return ret;
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

    public void setIdentity (ZFrame address)
    {
        if (this.identity != null)
            this.identity.destroy ();
        this.identity = address.duplicate ();
    }


    //  --------------------------------------------------------------------------
    //  Get/set the ZyniMsg id

    public int getType ()
    {
        return header.getType ();
    }

    public void setId (int id)
    {
        header = header.toBuilder ()
                        .setType (id)
                        .build ();
    }

    //  --------------------------------------------------------------------------
    //  Get/set the ZyniMsg signature

    public int getSignature ()
    {
        return header.getSignature ();
    }

    public Message getBody ()
    {
        return body;
    }

    public String dumps ()
    {
        StringBuilder builder = new StringBuilder ();
        builder.append (header.toString ());
        builder.append (body.toString ());

        return builder.toString ();
    }

    public static String addressToString (Address address)
    {
        InetAddress inetAddress = null;
        try {
            inetAddress = InetAddress.getByAddress (address.getAddr ().toByteArray ());
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException (e);
        }

        return String.format ("%s:%d", inetAddress.getHostAddress (), address.getPort ());
    }

    public static Address stringToAddress (String value)
    {
        String [] hp = value.split (":");
        Address.Builder builder = Address.newBuilder ();
        try {
            builder.setAddr (ByteString.copyFrom (InetAddress.getByName (hp [0]).getAddress ()));
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException ();
        }
        builder.setPort (Integer.valueOf (hp [1]));
        return builder.build ();
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
            VersionedValue vvalue = VersionedValue.newBuilder()
                                        .setVersion (version)
                                        .setValue (value)
                                    .build ();
            return put (key, vvalue);
        }

        public VersionedValue put (String key, String value)
        {
            int version = 0;

            VersionedValue ovalue = get (key);
            if (ovalue != null)
                version = ovalue.getVersion () + 1;

            return put (key, version, value);
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
