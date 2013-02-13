/*  =========================================================================
    GossipMsg.java
    
    Generated codec class for GossipMsg
    -------------------------------------------------------------------------
    Copyright (c) 2012-2013 InfiniLoop Corporation                                
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

/*  These are the gossip_msg messages
    HELLO - Greet a peer so it can connect back to us
        ipaddress     string
        mailbox       number 2
        groups        strings
        status        number 1
        headers       vdictionary
    WHISPER - Send a message to a peer
        content       frame
    SHOUT - Send a message to a group
        group         string
        content       frame
    JOIN - Join a group
        group         string
        status        number 1
    STATE - This is the Jini Gossip protocol raw version.
        address       address:port
        generation    number 4
        max-version   number 4
        extra         vdictionary
    PING - Ping a peer that has gone silent
        digest        [State, ...]
    PING_OK - Reply to a peer's ping
        request       [State, ...]
        response      [State, ...]
    PING_END - Reply to a peer's ping
        digest        [State, ...]
    EXIT - Notifiy a self exit
*/

package org.jeromq.jini.heartbeat;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.jeromq.ZFrame;
import org.jeromq.ZMQ.Socket;
import org.jeromq.ZMsg;
import org.jeromq.jini.JiniMsg;
import org.jeromq.jini.JiniMsg.VersionedMap;
import org.jeromq.jini.JiniMsg.VersionedValue;

//  Opaque class structure
public class GossipMsg
{
    public static final int GOSSIP_MSG_SIGNATURE = "gossip".hashCode ();
    static {
        JiniMsg.registerBuilder (GOSSIP_MSG_SIGNATURE, new GossipMsgBuilder ());
    }

    public static final int HELLO                 = 1;
    public static final int WHISPER               = 2;
    public static final int SHOUT                 = 3;
    public static final int JOIN                  = 4;
    public static final int STATE                 = 5;
    public static final int PING                  = 8;
    public static final int PING_OK               = 9;
    public static final int PING_END              = 10;
    public static final int EXIT                  = 11;

    public static class Hello extends JiniMsg
    {
        private String ipaddress;
        private int mailbox;
        private List <String> groups;
        private int status;
        private VersionedMap headers;

        public Hello ()
        {
            super (HELLO, GOSSIP_MSG_SIGNATURE);
        }

        public Hello (JiniMsg parent)
        {
            super (parent);
        }

        public Hello (
            String ipaddress,
            int mailbox,
            Collection <String> groups,
            int status,
            VersionedMap headers)
        {
            this ();
            this.setIpaddress (ipaddress);
            this.setMailbox (mailbox);
            this.setGroups (groups);
            this.setStatus (status);
            this.setHeaders (headers);
        }

        public void destroy ()
        {
            super.destroy ();
        }

        //  --------------------------------------------------------------------------
        //  Get/set the ipaddress field

        public String getIpaddress ()
        {
            return ipaddress;
        }

        public void setIpaddress (String format, Object ... args)
        {
            //  Format into newly allocated string
            ipaddress = String.format (format, args);
        }
        //  --------------------------------------------------------------------------
        //  Get/set the mailbox field

        public int getMailbox ()
        {
            return mailbox;
        }

        public void setMailbox (int mailbox)
        {
            this.mailbox = mailbox;
        }
        //  --------------------------------------------------------------------------
        //  Iterate through the groups field, and append a groups value

        public List <String> getGroups ()
        {
            return groups;
        }

        public void appendGroups (String format, Object ... args)
        {
            //  Format into newly allocated string
            
            String string = String.format (format, args);
            //  Attach string to list
            if (groups == null)
                groups = new ArrayList <String> ();
            groups.add (string);
        }

        public void setGroups (Collection <String> value)
        {
            groups = new ArrayList <String> (value); 
        }
        //  --------------------------------------------------------------------------
        //  Get/set the status field

        public int getStatus ()
        {
            return status;
        }

        public void setStatus (int status)
        {
            this.status = status;
        }
        //  --------------------------------------------------------------------------
        //  Get/set a versioned value in the headers dictionary

        public VersionedMap getHeaders ()
        {
            return headers;
        }

        public String getHeadersString (String key, String defaultValue)
        {
            VersionedValue vvalue = null;
            String value = defaultValue;
            if (headers != null)
                vvalue = headers.get (key);
            if (vvalue != null)
                value = vvalue.getValue ();

            return value;
        }

        public long getHeadersNumber (String key, long defaultValue)
        {
            long value = defaultValue;
            VersionedValue vvalue = null;
            if (headers != null)
                vvalue = headers.get (key);
            if (vvalue != null)
                value = Long.valueOf (vvalue.getValue ());

            return value;
        }

        public void insertHeaders (String key, int version, String format, Object ... args)
        {
            //  Format string into buffer
            String string = String.format (format, args);

            //  Store string in hash table
            if (headers == null)
                headers = new VersionedMap ();
            headers.put (key, version, string);
        }

        public void setHeaders (VersionedMap value)
        {
            if (headers == null)
                headers = new VersionedMap ();
            else
                headers = new VersionedMap (value); 
        }
        //  Count size of key=value pair
        private static int 
        headersCount (final Map.Entry <String, VersionedValue> entry)
        {
            return entry.getKey ().length () + 1 + 4 + entry.getValue ().getValue ().length () + 1;
        }

        //  Serialize headers key=value pair
        private static void
        headersWrite (final Map.Entry <String, VersionedValue> entry, Hello self)
        {
            self.putString (entry.getKey ());
            self.putNumber4 (entry.getValue ().getVersion ());
            self.putString (entry.getValue ().getValue ());
        }

        private static Hello build (JiniMsg parent, Socket input)
        {
            Hello self = new Hello (parent);

            self.ipaddress = self.getString ();
            self.mailbox = self.getNumber2 ();
            int groupsSize = self.getNumber1 ();
            self.groups = new ArrayList<String> ();
            while (groupsSize-- > 0) {
                String string = self.getString ();
                self.groups.add (string);
            }
            self.status = self.getNumber1 ();
            int headersSize = self.getNumber1 ();
            self.headers = new VersionedMap ();
            while (headersSize-- > 0) {
                String k = self.getString ();
                int ver = self.getNumber4 ();
                String v = self.getString ();
                self.headers.put (k, ver, v);
            }
            
            return self;
        }

        //  --------------------------------------------------------------------------
        //  Get frame size 

        @Override
        protected int getFrameSize ()
        {
            //  Calculate size of serialized data
            int frameSize = 0;
            //  ipaddress is a string with 1-byte length
            frameSize++;       //  Size is one octet
            if (ipaddress != null)
                frameSize += ipaddress.length ();
            //  mailbox is a 2-byte integer
            frameSize += 2;
            //  groups is an array of strings
            frameSize++;       //  Size is one octet
            if (groups != null) {
                for (String value : groups) 
                    frameSize += 1 + value.length ();
            }
            //  status is a 1-byte integer
            frameSize += 1;
            //  headers is an array of key=VersionedValue 
            frameSize++;       //  Size is one octet
            if (headers != null) {
                int headersBytes = 0;
                for (Map.Entry <String, VersionedValue> entry: headers.entrySet ()) {
                    headersBytes += headersCount (entry);
                }
                frameSize += headersBytes;
            }
            return frameSize;
        }

        //  --------------------------------------------------------------------------
        //  Serialize

        @Override
        protected ZMsg serialize ()
        {
            ZMsg msg = null;
            //  Now serialize message into the frame
            if (ipaddress != null)
                putString (ipaddress);
            else
                putNumber1 ((byte) 0);      //  Empty string
            putNumber2 (mailbox);
            if (groups != null) {
                putNumber1 ((byte) groups.size ());
                for (String value : groups) {
                    putString (value);
                }
            }
            else
                putNumber1 ((byte) 0);      //  Empty string array
            putNumber1 (status);
            if (headers != null) {
                putNumber1 ((byte) headers.size ());
                for (Map.Entry <String, VersionedValue> entry: headers.entrySet ()) {
                    headersWrite (entry, this);
                }
            }
            else
                putNumber1 ((byte) 0);      //  Empty dictionary
        
            return msg;
        }

        //  --------------------------------------------------------------------------
        //  Duplicate the GossipMsg message

        public Hello dup (Hello self)
        {
            if (self == null)
                return null;

            Hello copy = new Hello ();
            if (self.getIdentity () != null)
                copy.setAddress (self.getIdentity ());
            copy.ipaddress = self.ipaddress;
            copy.mailbox = self.mailbox;
            copy.groups = new ArrayList <String> (self.groups);
            copy.status = self.status;
            copy.headers = new VersionedMap (self.headers);
            return copy;
        }

        //  Dump headers key=value pair to stdout
        public static void headersDump (StringBuilder b, String tab, 
                                        Map.Entry <String, VersionedValue> entry, Hello self)
        {
            b.append (String.format ("%s    %s=[%d]%s\n", tab, entry.getKey (), entry.getValue ().getVersion (), entry.getValue ().getValue ()));
        }



        //  --------------------------------------------------------------------------
        //  Print contents of message to stdout

        public void dump ()
        {
            StringBuilder b = new StringBuilder ();
            dump (b, 0);
            System.out.print (b);
        }
        public String dumps ()
        {
            StringBuilder b = new StringBuilder ();
            dump (b, 0);
            return b.toString ();
        }

        private void dump (StringBuilder b, int depth)
        {
            String tab0 = "";
            String tab = "";
            for (int i = 0; i < depth; i++ )
                tab0 += "    ";
            tab = tab0 + "    ";

            b.append (String.format ("%sHELLO:\n", tab0));

            if (ipaddress != null)
                System.out.printf ("    ipaddress='%s'\n", ipaddress);
            else
                System.out.printf ("    ipaddress=\n");
            b.append (String.format ("%smailbox=%d\n", tab, (long)mailbox));
            System.out.printf ("    groups={");
            if (groups != null) {
                for (String value : groups) {
                    System.out.printf (" '%s'", value);
                }
            }
            System.out.printf (" }\n");
            b.append (String.format ("%sstatus=%d\n", tab, (long)status));
            b.append (String.format ("%sheaders={\n", tab));
            if (headers != null) {
                for (Map.Entry <String, VersionedValue> entry : headers.entrySet ())
                    headersDump (b, tab, entry, this);
            }
            b.append (String.format ("%s}\n", tab));
        }
    }
    
    public static class Whisper extends JiniMsg
    {
        private ZFrame content;

        public Whisper ()
        {
            super (WHISPER, GOSSIP_MSG_SIGNATURE);
        }

        public Whisper (JiniMsg parent)
        {
            super (parent);
        }

        public Whisper (
            ZFrame content)
        {
            this ();
            this.setContent (content.duplicate ());
        }

        public void destroy ()
        {
            super.destroy ();
        }

        //  --------------------------------------------------------------------------
        //  Get/set the content field

        public ZFrame getContent ()
        {
            return content;
        }

        //  Takes ownership of supplied frame
        public void setContent (ZFrame frame)
        {
            if (content != null)
                content.destroy ();
            content = frame;
        }

        private static Whisper build (JiniMsg parent, Socket input)
        {
            Whisper self = new Whisper (parent);

            //  Get next frame, leave current untouched
            if (!input.hasReceiveMore ())
                throw new IllegalArgumentException ();
            self.content = ZFrame.recvFrame (input);
            
            return self;
        }

        //  --------------------------------------------------------------------------
        //  Get frame size 

        @Override
        protected int getFrameSize ()
        {
            //  Calculate size of serialized data
            int frameSize = 0;
            return frameSize;
        }

        //  --------------------------------------------------------------------------
        //  Serialize

        @Override
        protected ZMsg serialize ()
        {
            ZMsg msg = null;
            //  Now serialize message into the frame
            msg = new ZMsg ();
        
            //  Now send any frame fields, in order
            //  If content isn't set, send an empty frame
            if (content == null)
                content = new ZFrame ("");
            msg.add (content);
            return msg;
        }

        //  --------------------------------------------------------------------------
        //  Duplicate the GossipMsg message

        public Whisper dup (Whisper self)
        {
            if (self == null)
                return null;

            Whisper copy = new Whisper ();
            if (self.getIdentity () != null)
                copy.setAddress (self.getIdentity ());
            copy.content = self.content.duplicate ();
            return copy;
        }



        //  --------------------------------------------------------------------------
        //  Print contents of message to stdout

        public void dump ()
        {
            StringBuilder b = new StringBuilder ();
            dump (b, 0);
            System.out.print (b);
        }
        public String dumps ()
        {
            StringBuilder b = new StringBuilder ();
            dump (b, 0);
            return b.toString ();
        }

        private void dump (StringBuilder b, int depth)
        {
            String tab0 = "";
            String tab = "";
            for (int i = 0; i < depth; i++ )
                tab0 += "    ";
            tab = tab0 + "    ";

            b.append (String.format ("%sWHISPER:\n", tab0));

            System.out.printf ("    content={\n");
            if (content != null) {
                int size = content.size ();
                byte [] data = content.getData ();
                System.out.printf ("        size=%d\n", content.size ());
                if (size > 32)
                    size = 32;
                int contentIndex;
                for (contentIndex = 0; contentIndex < size; contentIndex++) {
                    if (contentIndex != 0 && (contentIndex % 4 == 0))
                        System.out.printf ("-");
                    System.out.printf ("%02X", data [contentIndex]);
                }
            }
            System.out.printf ("    }\n");
        }
    }
    
    public static class Shout extends JiniMsg
    {
        private String group;
        private ZFrame content;

        public Shout ()
        {
            super (SHOUT, GOSSIP_MSG_SIGNATURE);
        }

        public Shout (JiniMsg parent)
        {
            super (parent);
        }

        public Shout (
            String group,
            ZFrame content)
        {
            this ();
            this.setGroup (group);
            this.setContent (content.duplicate ());
        }

        public void destroy ()
        {
            super.destroy ();
        }

        //  --------------------------------------------------------------------------
        //  Get/set the group field

        public String getGroup ()
        {
            return group;
        }

        public void setGroup (String format, Object ... args)
        {
            //  Format into newly allocated string
            group = String.format (format, args);
        }
        //  --------------------------------------------------------------------------
        //  Get/set the content field

        public ZFrame getContent ()
        {
            return content;
        }

        //  Takes ownership of supplied frame
        public void setContent (ZFrame frame)
        {
            if (content != null)
                content.destroy ();
            content = frame;
        }

        private static Shout build (JiniMsg parent, Socket input)
        {
            Shout self = new Shout (parent);

            self.group = self.getString ();
            //  Get next frame, leave current untouched
            if (!input.hasReceiveMore ())
                throw new IllegalArgumentException ();
            self.content = ZFrame.recvFrame (input);
            
            return self;
        }

        //  --------------------------------------------------------------------------
        //  Get frame size 

        @Override
        protected int getFrameSize ()
        {
            //  Calculate size of serialized data
            int frameSize = 0;
            //  group is a string with 1-byte length
            frameSize++;       //  Size is one octet
            if (group != null)
                frameSize += group.length ();
            return frameSize;
        }

        //  --------------------------------------------------------------------------
        //  Serialize

        @Override
        protected ZMsg serialize ()
        {
            ZMsg msg = null;
            //  Now serialize message into the frame
            if (group != null)
                putString (group);
            else
                putNumber1 ((byte) 0);      //  Empty string
            msg = new ZMsg ();
        
            //  Now send any frame fields, in order
            //  If content isn't set, send an empty frame
            if (content == null)
                content = new ZFrame ("");
            msg.add (content);
            return msg;
        }

        //  --------------------------------------------------------------------------
        //  Duplicate the GossipMsg message

        public Shout dup (Shout self)
        {
            if (self == null)
                return null;

            Shout copy = new Shout ();
            if (self.getIdentity () != null)
                copy.setAddress (self.getIdentity ());
            copy.group = self.group;
            copy.content = self.content.duplicate ();
            return copy;
        }



        //  --------------------------------------------------------------------------
        //  Print contents of message to stdout

        public void dump ()
        {
            StringBuilder b = new StringBuilder ();
            dump (b, 0);
            System.out.print (b);
        }
        public String dumps ()
        {
            StringBuilder b = new StringBuilder ();
            dump (b, 0);
            return b.toString ();
        }

        private void dump (StringBuilder b, int depth)
        {
            String tab0 = "";
            String tab = "";
            for (int i = 0; i < depth; i++ )
                tab0 += "    ";
            tab = tab0 + "    ";

            b.append (String.format ("%sSHOUT:\n", tab0));

            if (group != null)
                System.out.printf ("    group='%s'\n", group);
            else
                System.out.printf ("    group=\n");
            System.out.printf ("    content={\n");
            if (content != null) {
                int size = content.size ();
                byte [] data = content.getData ();
                System.out.printf ("        size=%d\n", content.size ());
                if (size > 32)
                    size = 32;
                int contentIndex;
                for (contentIndex = 0; contentIndex < size; contentIndex++) {
                    if (contentIndex != 0 && (contentIndex % 4 == 0))
                        System.out.printf ("-");
                    System.out.printf ("%02X", data [contentIndex]);
                }
            }
            System.out.printf ("    }\n");
        }
    }
    
    public static class Join extends JiniMsg
    {
        private String group;
        private int status;

        public Join ()
        {
            super (JOIN, GOSSIP_MSG_SIGNATURE);
        }

        public Join (JiniMsg parent)
        {
            super (parent);
        }

        public Join (
            String group,
            int status)
        {
            this ();
            this.setGroup (group);
            this.setStatus (status);
        }

        public void destroy ()
        {
            super.destroy ();
        }

        //  --------------------------------------------------------------------------
        //  Get/set the group field

        public String getGroup ()
        {
            return group;
        }

        public void setGroup (String format, Object ... args)
        {
            //  Format into newly allocated string
            group = String.format (format, args);
        }
        //  --------------------------------------------------------------------------
        //  Get/set the status field

        public int getStatus ()
        {
            return status;
        }

        public void setStatus (int status)
        {
            this.status = status;
        }

        private static Join build (JiniMsg parent, Socket input)
        {
            Join self = new Join (parent);

            self.group = self.getString ();
            self.status = self.getNumber1 ();
            
            return self;
        }

        //  --------------------------------------------------------------------------
        //  Get frame size 

        @Override
        protected int getFrameSize ()
        {
            //  Calculate size of serialized data
            int frameSize = 0;
            //  group is a string with 1-byte length
            frameSize++;       //  Size is one octet
            if (group != null)
                frameSize += group.length ();
            //  status is a 1-byte integer
            frameSize += 1;
            return frameSize;
        }

        //  --------------------------------------------------------------------------
        //  Serialize

        @Override
        protected ZMsg serialize ()
        {
            ZMsg msg = null;
            //  Now serialize message into the frame
            if (group != null)
                putString (group);
            else
                putNumber1 ((byte) 0);      //  Empty string
            putNumber1 (status);
        
            return msg;
        }

        //  --------------------------------------------------------------------------
        //  Duplicate the GossipMsg message

        public Join dup (Join self)
        {
            if (self == null)
                return null;

            Join copy = new Join ();
            if (self.getIdentity () != null)
                copy.setAddress (self.getIdentity ());
            copy.group = self.group;
            copy.status = self.status;
            return copy;
        }



        //  --------------------------------------------------------------------------
        //  Print contents of message to stdout

        public void dump ()
        {
            StringBuilder b = new StringBuilder ();
            dump (b, 0);
            System.out.print (b);
        }
        public String dumps ()
        {
            StringBuilder b = new StringBuilder ();
            dump (b, 0);
            return b.toString ();
        }

        private void dump (StringBuilder b, int depth)
        {
            String tab0 = "";
            String tab = "";
            for (int i = 0; i < depth; i++ )
                tab0 += "    ";
            tab = tab0 + "    ";

            b.append (String.format ("%sJOIN:\n", tab0));

            if (group != null)
                System.out.printf ("    group='%s'\n", group);
            else
                System.out.printf ("    group=\n");
            b.append (String.format ("%sstatus=%d\n", tab, (long)status));
        }
    }
    
    public static class State extends JiniMsg
    {
        private InetAddress address;
        private int addressPort;
        private int generation;
        private int max_version;
        private VersionedMap extra;

        public State ()
        {
            super (STATE, GOSSIP_MSG_SIGNATURE);
        }

        public State (JiniMsg parent)
        {
            super (parent);
        }

        public State (
            String address,
            int generation,
            int max_version,
            VersionedMap extra)
        {
            this ();
            this.setAddress (address);
            this.setGeneration (generation);
            this.setMaxVersion (max_version);
            this.setExtra (extra);
        }

        public void destroy ()
        {
            super.destroy ();
        }

        //  --------------------------------------------------------------------------
        //  Get/set a address value in the address and addressPort

        public InetAddress getAddress ()
        {
            return address;
        }

        public String getAddressString ()
        {
            return address.getHostAddress () + ":" + addressPort;
        }

        public int getAddressPort ()
        {
            return addressPort;
        }

        public void setAddress (InetAddress value)
        {
            address = value; 
        }

        public void setAddressPort (int value)
        {
            addressPort = value; 
        }

        public void setAddress (String value)
        {
            String [] hp = value.split (":");
            try {
                address = InetAddress.getByName (hp [0]); 
            } catch (UnknownHostException e) {
                throw new IllegalArgumentException ();
            }
            addressPort = Integer.valueOf (hp [1]); 
        }
        //  --------------------------------------------------------------------------
        //  Get/set the generation field

        public int getGeneration ()
        {
            return generation;
        }

        public void setGeneration (int generation)
        {
            this.generation = generation;
        }
        //  --------------------------------------------------------------------------
        //  Get/set the max_version field

        public int getMaxVersion ()
        {
            return max_version;
        }

        public void setMaxVersion (int max_version)
        {
            this.max_version = max_version;
        }
        //  --------------------------------------------------------------------------
        //  Get/set a versioned value in the extra dictionary

        public VersionedMap getExtra ()
        {
            return extra;
        }

        public String getExtraString (String key, String defaultValue)
        {
            VersionedValue vvalue = null;
            String value = defaultValue;
            if (extra != null)
                vvalue = extra.get (key);
            if (vvalue != null)
                value = vvalue.getValue ();

            return value;
        }

        public long getExtraNumber (String key, long defaultValue)
        {
            long value = defaultValue;
            VersionedValue vvalue = null;
            if (extra != null)
                vvalue = extra.get (key);
            if (vvalue != null)
                value = Long.valueOf (vvalue.getValue ());

            return value;
        }

        public void insertExtra (String key, int version, String format, Object ... args)
        {
            //  Format string into buffer
            String string = String.format (format, args);

            //  Store string in hash table
            if (extra == null)
                extra = new VersionedMap ();
            extra.put (key, version, string);
        }

        public void setExtra (VersionedMap value)
        {
            if (extra == null)
                extra = new VersionedMap ();
            else
                extra = new VersionedMap (value); 
        }
        //  Count size of key=value pair
        private static int 
        extraCount (final Map.Entry <String, VersionedValue> entry)
        {
            return entry.getKey ().length () + 1 + 4 + entry.getValue ().getValue ().length () + 1;
        }

        //  Serialize extra key=value pair
        private static void
        extraWrite (final Map.Entry <String, VersionedValue> entry, State self)
        {
            self.putString (entry.getKey ());
            self.putNumber4 (entry.getValue ().getVersion ());
            self.putString (entry.getValue ().getValue ());
        }

        private static State build (JiniMsg parent, Socket input)
        {
            State self = new State (parent);

            byte [] addr_ = self.getBlock (4);
            try {
                self.address = InetAddress.getByAddress (addr_);
            } catch (UnknownHostException e) {
                throw new IllegalArgumentException ();
            }
            self.addressPort = self.unsigned (self.getNumber2 ());
            self.generation = self.getNumber4 ();
            self.max_version = self.getNumber4 ();
            int extraSize = self.getNumber1 ();
            self.extra = new VersionedMap ();
            while (extraSize-- > 0) {
                String k = self.getString ();
                int ver = self.getNumber4 ();
                String v = self.getString ();
                self.extra.put (k, ver, v);
            }
            
            return self;
        }

        //  --------------------------------------------------------------------------
        //  Get frame size 

        @Override
        protected int getFrameSize ()
        {
            //  Calculate size of serialized data
            int frameSize = 0;
            frameSize += 6;       //  Size is 6 octets
            //  generation is a 4-byte integer
            frameSize += 4;
            //  max_version is a 4-byte integer
            frameSize += 4;
            //  extra is an array of key=VersionedValue 
            frameSize++;       //  Size is one octet
            if (extra != null) {
                int extraBytes = 0;
                for (Map.Entry <String, VersionedValue> entry: extra.entrySet ()) {
                    extraBytes += extraCount (entry);
                }
                frameSize += extraBytes;
            }
            return frameSize;
        }

        //  --------------------------------------------------------------------------
        //  Serialize

        @Override
        protected ZMsg serialize ()
        {
            ZMsg msg = null;
            //  Now serialize message into the frame
            if (address != null) {
                putBlock (address.getAddress (), 4);
                putNumber2 (addressPort);
            }
            else
                putBlock (new byte [] {0,0,0,0,0,0}, 6);
            putNumber4 (generation);
            putNumber4 (max_version);
            if (extra != null) {
                putNumber1 ((byte) extra.size ());
                for (Map.Entry <String, VersionedValue> entry: extra.entrySet ()) {
                    extraWrite (entry, this);
                }
            }
            else
                putNumber1 ((byte) 0);      //  Empty dictionary
        
            return msg;
        }

        //  --------------------------------------------------------------------------
        //  Duplicate the GossipMsg message

        public State dup (State self)
        {
            if (self == null)
                return null;

            State copy = new State ();
            if (self.getIdentity () != null)
                copy.setAddress (self.getIdentity ());
            copy.address = self.address;
            copy.addressPort = self.addressPort;
            copy.generation = self.generation;
            copy.max_version = self.max_version;
            copy.extra = new VersionedMap (self.extra);
            return copy;
        }

        //  Dump extra key=value pair to stdout
        public static void extraDump (StringBuilder b, String tab, 
                                        Map.Entry <String, VersionedValue> entry, State self)
        {
            b.append (String.format ("%s    %s=[%d]%s\n", tab, entry.getKey (), entry.getValue ().getVersion (), entry.getValue ().getValue ()));
        }



        //  --------------------------------------------------------------------------
        //  Print contents of message to stdout

        public void dump ()
        {
            StringBuilder b = new StringBuilder ();
            dump (b, 0);
            System.out.print (b);
        }
        public String dumps ()
        {
            StringBuilder b = new StringBuilder ();
            dump (b, 0);
            return b.toString ();
        }

        private void dump (StringBuilder b, int depth)
        {
            String tab0 = "";
            String tab = "";
            for (int i = 0; i < depth; i++ )
                tab0 += "    ";
            tab = tab0 + "    ";

            b.append (String.format ("%sSTATE:\n", tab0));

            b.append (String.format ("%saddress=%s\n", tab, getAddressString ()));
            b.append (String.format ("%sgeneration=%d\n", tab, (long)generation));
            b.append (String.format ("%smax_version=%d\n", tab, (long)max_version));
            b.append (String.format ("%sextra={\n", tab));
            if (extra != null) {
                for (Map.Entry <String, VersionedValue> entry : extra.entrySet ())
                    extraDump (b, tab, entry, this);
            }
            b.append (String.format ("%s}\n", tab));
        }
    }
    
    public static class Ping extends JiniMsg
    {
        private List <State> digest;

        public Ping ()
        {
            super (PING, GOSSIP_MSG_SIGNATURE);
        }

        public Ping (JiniMsg parent)
        {
            super (parent);
        }

        public Ping (
            Collection <State> digest)
        {
            this ();
            this.setDigest (digest);
        }

        public void destroy ()
        {
            super.destroy ();
        }

        //  --------------------------------------------------------------------------
        //  Iterate through the digest field, and append a digest value

        public List <State> getDigest ()
        {
            return digest;
        }

        public void appendDigest (State value)
        {
            if (digest == null)
                digest = new ArrayList <State> ();
            digest.add (value);
        }

        public void setDigest (Collection <State> value)
        {
            digest = new ArrayList <State> (value); 
        }

        private static Ping build (JiniMsg parent, Socket input)
        {
            Ping self = new Ping (parent);

            int digestSize = self.getNumber1 ();
            self.digest = new ArrayList<State> ();
            while (digestSize-- > 0) {
                State digestItem = State.build (parent, input);
                self.digest.add (digestItem);
            }
            
            return self;
        }

        //  --------------------------------------------------------------------------
        //  Get frame size 

        @Override
        protected int getFrameSize ()
        {
            //  Calculate size of serialized data
            int frameSize = 0;
            //  digest is an array of reference
            frameSize++;       //  Size is one octet
            if (digest != null) {
                for (State value : digest) 
                    frameSize += value.getFrameSize ();
            }
            return frameSize;
        }

        //  --------------------------------------------------------------------------
        //  Serialize

        @Override
        protected ZMsg serialize ()
        {
            ZMsg msg = null;
            //  Now serialize message into the frame
            if (digest != null) {
                putNumber1 ((byte) digest.size ());
                for (State value : digest) {
                    value.setNeedle (getNeedle ());
                    value.serialize ();
                }
            }
            else
                putNumber1 ((byte) 0);      //  Empty reference array
        
            return msg;
        }

        //  --------------------------------------------------------------------------
        //  Duplicate the GossipMsg message

        public Ping dup (Ping self)
        {
            if (self == null)
                return null;

            Ping copy = new Ping ();
            if (self.getIdentity () != null)
                copy.setAddress (self.getIdentity ());
            copy.digest = new ArrayList <State> (self.digest);
            return copy;
        }



        //  --------------------------------------------------------------------------
        //  Print contents of message to stdout

        public void dump ()
        {
            StringBuilder b = new StringBuilder ();
            dump (b, 0);
            System.out.print (b);
        }
        public String dumps ()
        {
            StringBuilder b = new StringBuilder ();
            dump (b, 0);
            return b.toString ();
        }

        private void dump (StringBuilder b, int depth)
        {
            String tab0 = "";
            String tab = "";
            for (int i = 0; i < depth; i++ )
                tab0 += "    ";
            tab = tab0 + "    ";

            b.append (String.format ("%sPING:\n", tab0));

            b.append (String.format ("%sdigest={\n", tab));
            if (digest != null) {
                for (State value : digest) {
                    value.dump (b, depth + 1);
                }
            }
            b.append (String.format ("%s}\n", tab));
        }
    }
    
    public static class PingOk extends JiniMsg
    {
        private List <State> request;
        private List <State> response;

        public PingOk ()
        {
            super (PING_OK, GOSSIP_MSG_SIGNATURE);
        }

        public PingOk (JiniMsg parent)
        {
            super (parent);
        }

        public PingOk (
            Collection <State> request,
            Collection <State> response)
        {
            this ();
            this.setRequest (request);
            this.setResponse (response);
        }

        public void destroy ()
        {
            super.destroy ();
        }

        //  --------------------------------------------------------------------------
        //  Iterate through the request field, and append a request value

        public List <State> getRequest ()
        {
            return request;
        }

        public void appendRequest (State value)
        {
            if (request == null)
                request = new ArrayList <State> ();
            request.add (value);
        }

        public void setRequest (Collection <State> value)
        {
            request = new ArrayList <State> (value); 
        }
        //  --------------------------------------------------------------------------
        //  Iterate through the response field, and append a response value

        public List <State> getResponse ()
        {
            return response;
        }

        public void appendResponse (State value)
        {
            if (response == null)
                response = new ArrayList <State> ();
            response.add (value);
        }

        public void setResponse (Collection <State> value)
        {
            response = new ArrayList <State> (value); 
        }

        private static PingOk build (JiniMsg parent, Socket input)
        {
            PingOk self = new PingOk (parent);

            int requestSize = self.getNumber1 ();
            self.request = new ArrayList<State> ();
            while (requestSize-- > 0) {
                State requestItem = State.build (parent, input);
                self.request.add (requestItem);
            }
            int responseSize = self.getNumber1 ();
            self.response = new ArrayList<State> ();
            while (responseSize-- > 0) {
                State responseItem = State.build (parent, input);
                self.response.add (responseItem);
            }
            
            return self;
        }

        //  --------------------------------------------------------------------------
        //  Get frame size 

        @Override
        protected int getFrameSize ()
        {
            //  Calculate size of serialized data
            int frameSize = 0;
            //  request is an array of reference
            frameSize++;       //  Size is one octet
            if (request != null) {
                for (State value : request) 
                    frameSize += value.getFrameSize ();
            }
            //  response is an array of reference
            frameSize++;       //  Size is one octet
            if (response != null) {
                for (State value : response) 
                    frameSize += value.getFrameSize ();
            }
            return frameSize;
        }

        //  --------------------------------------------------------------------------
        //  Serialize

        @Override
        protected ZMsg serialize ()
        {
            ZMsg msg = null;
            //  Now serialize message into the frame
            if (request != null) {
                putNumber1 ((byte) request.size ());
                for (State value : request) {
                    value.setNeedle (getNeedle ());
                    value.serialize ();
                }
            }
            else
                putNumber1 ((byte) 0);      //  Empty reference array
            if (response != null) {
                putNumber1 ((byte) response.size ());
                for (State value : response) {
                    value.setNeedle (getNeedle ());
                    value.serialize ();
                }
            }
            else
                putNumber1 ((byte) 0);      //  Empty reference array
        
            return msg;
        }

        //  --------------------------------------------------------------------------
        //  Duplicate the GossipMsg message

        public PingOk dup (PingOk self)
        {
            if (self == null)
                return null;

            PingOk copy = new PingOk ();
            if (self.getIdentity () != null)
                copy.setAddress (self.getIdentity ());
            copy.request = new ArrayList <State> (self.request);
            copy.response = new ArrayList <State> (self.response);
            return copy;
        }



        //  --------------------------------------------------------------------------
        //  Print contents of message to stdout

        public void dump ()
        {
            StringBuilder b = new StringBuilder ();
            dump (b, 0);
            System.out.print (b);
        }
        public String dumps ()
        {
            StringBuilder b = new StringBuilder ();
            dump (b, 0);
            return b.toString ();
        }

        private void dump (StringBuilder b, int depth)
        {
            String tab0 = "";
            String tab = "";
            for (int i = 0; i < depth; i++ )
                tab0 += "    ";
            tab = tab0 + "    ";

            b.append (String.format ("%sPING_OK:\n", tab0));

            b.append (String.format ("%srequest={\n", tab));
            if (request != null) {
                for (State value : request) {
                    value.dump (b, depth + 1);
                }
            }
            b.append (String.format ("%s}\n", tab));
            b.append (String.format ("%sresponse={\n", tab));
            if (response != null) {
                for (State value : response) {
                    value.dump (b, depth + 1);
                }
            }
            b.append (String.format ("%s}\n", tab));
        }
    }
    
    public static class PingEnd extends JiniMsg
    {
        private List <State> digest;

        public PingEnd ()
        {
            super (PING_END, GOSSIP_MSG_SIGNATURE);
        }

        public PingEnd (JiniMsg parent)
        {
            super (parent);
        }

        public PingEnd (
            Collection <State> digest)
        {
            this ();
            this.setDigest (digest);
        }

        public void destroy ()
        {
            super.destroy ();
        }

        //  --------------------------------------------------------------------------
        //  Iterate through the digest field, and append a digest value

        public List <State> getDigest ()
        {
            return digest;
        }

        public void appendDigest (State value)
        {
            if (digest == null)
                digest = new ArrayList <State> ();
            digest.add (value);
        }

        public void setDigest (Collection <State> value)
        {
            digest = new ArrayList <State> (value); 
        }

        private static PingEnd build (JiniMsg parent, Socket input)
        {
            PingEnd self = new PingEnd (parent);

            int digestSize = self.getNumber1 ();
            self.digest = new ArrayList<State> ();
            while (digestSize-- > 0) {
                State digestItem = State.build (parent, input);
                self.digest.add (digestItem);
            }
            
            return self;
        }

        //  --------------------------------------------------------------------------
        //  Get frame size 

        @Override
        protected int getFrameSize ()
        {
            //  Calculate size of serialized data
            int frameSize = 0;
            //  digest is an array of reference
            frameSize++;       //  Size is one octet
            if (digest != null) {
                for (State value : digest) 
                    frameSize += value.getFrameSize ();
            }
            return frameSize;
        }

        //  --------------------------------------------------------------------------
        //  Serialize

        @Override
        protected ZMsg serialize ()
        {
            ZMsg msg = null;
            //  Now serialize message into the frame
            if (digest != null) {
                putNumber1 ((byte) digest.size ());
                for (State value : digest) {
                    value.setNeedle (getNeedle ());
                    value.serialize ();
                }
            }
            else
                putNumber1 ((byte) 0);      //  Empty reference array
        
            return msg;
        }

        //  --------------------------------------------------------------------------
        //  Duplicate the GossipMsg message

        public PingEnd dup (PingEnd self)
        {
            if (self == null)
                return null;

            PingEnd copy = new PingEnd ();
            if (self.getIdentity () != null)
                copy.setAddress (self.getIdentity ());
            copy.digest = new ArrayList <State> (self.digest);
            return copy;
        }



        //  --------------------------------------------------------------------------
        //  Print contents of message to stdout

        public void dump ()
        {
            StringBuilder b = new StringBuilder ();
            dump (b, 0);
            System.out.print (b);
        }
        public String dumps ()
        {
            StringBuilder b = new StringBuilder ();
            dump (b, 0);
            return b.toString ();
        }

        private void dump (StringBuilder b, int depth)
        {
            String tab0 = "";
            String tab = "";
            for (int i = 0; i < depth; i++ )
                tab0 += "    ";
            tab = tab0 + "    ";

            b.append (String.format ("%sPING_END:\n", tab0));

            b.append (String.format ("%sdigest={\n", tab));
            if (digest != null) {
                for (State value : digest) {
                    value.dump (b, depth + 1);
                }
            }
            b.append (String.format ("%s}\n", tab));
        }
    }
    
    public static class Exit extends JiniMsg
    {

        public Exit ()
        {
            super (EXIT, GOSSIP_MSG_SIGNATURE);
        }

        public Exit (JiniMsg parent)
        {
            super (parent);
        }


        public void destroy ()
        {
            super.destroy ();
        }


        private static Exit build (JiniMsg parent, Socket input)
        {
            Exit self = new Exit (parent);

            
            return self;
        }

        //  --------------------------------------------------------------------------
        //  Get frame size 

        @Override
        protected int getFrameSize ()
        {
            //  Calculate size of serialized data
            int frameSize = 0;
            return frameSize;
        }

        //  --------------------------------------------------------------------------
        //  Serialize

        @Override
        protected ZMsg serialize ()
        {
            ZMsg msg = null;
            //  Now serialize message into the frame
        
            return msg;
        }

        //  --------------------------------------------------------------------------
        //  Duplicate the GossipMsg message

        public Exit dup (Exit self)
        {
            if (self == null)
                return null;

            Exit copy = new Exit ();
            if (self.getIdentity () != null)
                copy.setAddress (self.getIdentity ());
            return copy;
        }



        //  --------------------------------------------------------------------------
        //  Print contents of message to stdout

        public void dump ()
        {
            StringBuilder b = new StringBuilder ();
            dump (b, 0);
            System.out.print (b);
        }
        public String dumps ()
        {
            StringBuilder b = new StringBuilder ();
            dump (b, 0);
            return b.toString ();
        }

        private void dump (StringBuilder b, int depth)
        {
            String tab0 = "";
            String tab = "";
            for (int i = 0; i < depth; i++ )
                tab0 += "    ";
            tab = tab0 + "    ";

            b.append (String.format ("%sEXIT:\n", tab0));

        }
    }
    

    //  --------------------------------------------------------------------------
    //  Receive and parse a GossipMsg from the socket. Returns new object or
    //  null if error. Will block if there's no message waiting.

    public static class GossipMsgBuilder 
                              implements JiniMsg.Builder
    {
        @Override
        public JiniMsg build (JiniMsg parent, Socket input)
        {
            assert (parent != null);

            JiniMsg self = null;
            switch (parent.getId ()) {
            case HELLO:
                self = Hello.build (parent, input);
                break;

            case WHISPER:
                self = Whisper.build (parent, input);
                break;

            case SHOUT:
                self = Shout.build (parent, input);
                break;

            case JOIN:
                self = Join.build (parent, input);
                break;

            case STATE:
                self = State.build (parent, input);
                break;

            case PING:
                self = Ping.build (parent, input);
                break;

            case PING_OK:
                self = PingOk.build (parent, input);
                break;

            case PING_END:
                self = PingEnd.build (parent, input);
                break;

            case EXIT:
                self = Exit.build (parent, input);
                break;

            default:
                throw new IllegalArgumentException ();
            }

            return self;
        }
    }

}

