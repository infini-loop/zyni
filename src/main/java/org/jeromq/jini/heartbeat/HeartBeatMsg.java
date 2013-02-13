/*  =========================================================================
    HeartBeatMsg.java
    
    Generated codec class for HeartBeatMsg
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

/*  These are the heart_beat_msg messages
    HELLO - Greet a peer so it can connect back to us
        sequence      number 2
        peers         strings
        headers       dictionary
    WHISPER - Send a message to a peer
        sequence      number 2
        content       frame
    SHOUT - Send a message to a group
        sequence      number 2
        group         string
        content       frame
    JOIN - Join a group
        sequence      number 2
        group         string
        status        number 1
    LEAVE - Leave a group
        sequence      number 2
        group         string
        status        number 1
    PING - Ping a peer that has gone silent
        sequence      number 2
    PING_OK - Reply to a peer's ping
        sequence      number 2
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
public class HeartBeatMsg
{
    public static final int HEART_BEAT_MSG_SIGNATURE = "heartbeat".hashCode ();
    static {
        JiniMsg.registerBuilder (HEART_BEAT_MSG_SIGNATURE, new HeartBeatMsgBuilder ());
    }

    public static final int HELLO                 = 1;
    public static final int WHISPER               = 2;
    public static final int SHOUT                 = 3;
    public static final int JOIN                  = 4;
    public static final int LEAVE                 = 5;
    public static final int PING                  = 6;
    public static final int PING_OK               = 7;

    public static class Hello extends JiniMsg
    {
        private int sequence;
        private List <String> peers;
        private Map <String, String> headers;

        public Hello ()
        {
            super (HELLO, HEART_BEAT_MSG_SIGNATURE);
        }

        public Hello (JiniMsg parent)
        {
            super (parent);
        }

        public Hello (
            int sequence,
            Collection <String> peers,
            Map <String, String> headers)
        {
            this ();
            this.setSequence (sequence);
            this.setPeers (peers);
            this.setHeaders (headers);
        }

        public void destroy ()
        {
            super.destroy ();
        }

        //  --------------------------------------------------------------------------
        //  Get/set the sequence field

        public int getSequence ()
        {
            return sequence;
        }

        public void setSequence (int sequence)
        {
            this.sequence = sequence;
        }
        //  --------------------------------------------------------------------------
        //  Iterate through the peers field, and append a peers value

        public List <String> getPeers ()
        {
            return peers;
        }

        public void appendPeers (String format, Object ... args)
        {
            //  Format into newly allocated string
            
            String string = String.format (format, args);
            //  Attach string to list
            if (peers == null)
                peers = new ArrayList <String> ();
            peers.add (string);
        }

        public void setPeers (Collection <String> value)
        {
            peers = new ArrayList <String> (value); 
        }
        //  --------------------------------------------------------------------------
        //  Get/set a value in the headers dictionary

        public Map <String, String> getHeaders ()
        {
            return headers;
        }

        public String getHeadersString (String key, String defaultValue)
        {
            String value = null;
            if (headers != null)
                value = headers.get (key);
            if (value == null)
                value = defaultValue;

            return value;
        }

        public long getHeadersNumber (String key, long defaultValue)
        {
            long value = defaultValue;
            String string = null;
            if (headers != null)
                string = headers.get (key);
            if (string != null)
                value = Long.valueOf (string);

            return value;
        }

        public void insertHeaders (String key, String format, Object ... args)
        {
            //  Format string into buffer
            String string = String.format (format, args);

            //  Store string in hash table
            if (headers == null)
                headers = new HashMap <String, String> ();
            headers.put (key, string);
        }

        public void setHeaders (Map <String, String> value)
        {
            headers = new HashMap <String, String> (value); 
        }

        public void setUnversionedHeaders (Map <String, VersionedValue> value)
        {
            headers = new HashMap <String, String> ();
            for (Map.Entry <String, VersionedValue> entry : value.entrySet ()) {
                headers.put (entry.getKey (), entry.getValue ().getValue ());
            }
        }
        //  Count size of key=value pair
        private static int 
        headersCount (final Map.Entry <String, String> entry)
        {
            return 1 + entry.getKey ().length () + 1 + entry.getValue ().length ();
        }

        //  Serialize headers key=value pair
        private static void
        headersWrite (final Map.Entry <String, String> entry, Hello self)
        {
            String string = entry.getKey () + "=" + entry.getValue ();
            self.putString (string);
        }

        private static Hello build (JiniMsg parent, Socket input)
        {
            Hello self = new Hello (parent);

            self.sequence = self.getNumber2 ();
            int peersSize = self.getNumber1 ();
            self.peers = new ArrayList<String> ();
            while (peersSize-- > 0) {
                String string = self.getString ();
                self.peers.add (string);
            }
            int headersSize = self.getNumber1 ();
            self.headers = new HashMap <String, String> ();
            while (headersSize-- > 0) {
                String string = self.getString ();
                String [] kv = string.split("=");
                self.headers.put(kv[0], kv[1]);
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
            //  sequence is a 2-byte integer
            frameSize += 2;
            //  peers is an array of strings
            frameSize++;       //  Size is one octet
            if (peers != null) {
                for (String value : peers) 
                    frameSize += 1 + value.length ();
            }
            //  headers is an array of key=value strings
            frameSize++;       //  Size is one octet
            if (headers != null) {
                int headersBytes = 0;
                for (Map.Entry <String, String> entry: headers.entrySet ()) {
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
            putNumber2 (sequence);
            if (peers != null) {
                putNumber1 ((byte) peers.size ());
                for (String value : peers) {
                    putString (value);
                }
            }
            else
                putNumber1 ((byte) 0);      //  Empty string array
            if (headers != null) {
                putNumber1 ((byte) headers.size ());
                for (Map.Entry <String, String> entry: headers.entrySet ()) {
                    headersWrite (entry, this);
                }
            }
            else
                putNumber1 ((byte) 0);      //  Empty dictionary
        
            return msg;
        }

        //  --------------------------------------------------------------------------
        //  Duplicate the HeartBeatMsg message

        public Hello dup (Hello self)
        {
            if (self == null)
                return null;

            Hello copy = new Hello ();
            if (self.getIdentity () != null)
                copy.setAddress (self.getIdentity ());
            copy.sequence = self.sequence;
            copy.peers = new ArrayList <String> (self.peers);
            copy.headers = new HashMap <String, String> (self.headers);
            return copy;
        }

        //  Dump headers key=value pair to stdout
        public static void headersDump (Map.Entry <String, String> entry, Hello self)
        {
            System.out.printf ("        %s=%s\n", entry.getKey (), entry.getValue ());
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

            b.append (String.format ("%ssequence=%d\n", tab, (long)sequence));
            System.out.printf ("    peers={");
            if (peers != null) {
                for (String value : peers) {
                    System.out.printf (" '%s'", value);
                }
            }
            System.out.printf (" }\n");
            System.out.printf ("    headers={\n");
            if (headers != null) {
                for (Map.Entry <String, String> entry : headers.entrySet ())
                    headersDump (entry, this);
            }
            System.out.printf ("    }\n");
        }
    }
    
    public static class Whisper extends JiniMsg
    {
        private int sequence;
        private ZFrame content;

        public Whisper ()
        {
            super (WHISPER, HEART_BEAT_MSG_SIGNATURE);
        }

        public Whisper (JiniMsg parent)
        {
            super (parent);
        }

        public Whisper (
            int sequence,
            ZFrame content)
        {
            this ();
            this.setSequence (sequence);
            this.setContent (content.duplicate ());
        }

        public void destroy ()
        {
            super.destroy ();
        }

        //  --------------------------------------------------------------------------
        //  Get/set the sequence field

        public int getSequence ()
        {
            return sequence;
        }

        public void setSequence (int sequence)
        {
            this.sequence = sequence;
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

            self.sequence = self.getNumber2 ();
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
            //  sequence is a 2-byte integer
            frameSize += 2;
            return frameSize;
        }

        //  --------------------------------------------------------------------------
        //  Serialize

        @Override
        protected ZMsg serialize ()
        {
            ZMsg msg = null;
            //  Now serialize message into the frame
            putNumber2 (sequence);
            msg = new ZMsg ();
        
            //  Now send any frame fields, in order
            //  If content isn't set, send an empty frame
            if (content == null)
                content = new ZFrame ("");
            msg.add (content);
            return msg;
        }

        //  --------------------------------------------------------------------------
        //  Duplicate the HeartBeatMsg message

        public Whisper dup (Whisper self)
        {
            if (self == null)
                return null;

            Whisper copy = new Whisper ();
            if (self.getIdentity () != null)
                copy.setAddress (self.getIdentity ());
            copy.sequence = self.sequence;
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

            b.append (String.format ("%ssequence=%d\n", tab, (long)sequence));
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
        private int sequence;
        private String group;
        private ZFrame content;

        public Shout ()
        {
            super (SHOUT, HEART_BEAT_MSG_SIGNATURE);
        }

        public Shout (JiniMsg parent)
        {
            super (parent);
        }

        public Shout (
            int sequence,
            String group,
            ZFrame content)
        {
            this ();
            this.setSequence (sequence);
            this.setGroup (group);
            this.setContent (content.duplicate ());
        }

        public void destroy ()
        {
            super.destroy ();
        }

        //  --------------------------------------------------------------------------
        //  Get/set the sequence field

        public int getSequence ()
        {
            return sequence;
        }

        public void setSequence (int sequence)
        {
            this.sequence = sequence;
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

            self.sequence = self.getNumber2 ();
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
            //  sequence is a 2-byte integer
            frameSize += 2;
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
            putNumber2 (sequence);
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
        //  Duplicate the HeartBeatMsg message

        public Shout dup (Shout self)
        {
            if (self == null)
                return null;

            Shout copy = new Shout ();
            if (self.getIdentity () != null)
                copy.setAddress (self.getIdentity ());
            copy.sequence = self.sequence;
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

            b.append (String.format ("%ssequence=%d\n", tab, (long)sequence));
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
        private int sequence;
        private String group;
        private int status;

        public Join ()
        {
            super (JOIN, HEART_BEAT_MSG_SIGNATURE);
        }

        public Join (JiniMsg parent)
        {
            super (parent);
        }

        public Join (
            int sequence,
            String group,
            int status)
        {
            this ();
            this.setSequence (sequence);
            this.setGroup (group);
            this.setStatus (status);
        }

        public void destroy ()
        {
            super.destroy ();
        }

        //  --------------------------------------------------------------------------
        //  Get/set the sequence field

        public int getSequence ()
        {
            return sequence;
        }

        public void setSequence (int sequence)
        {
            this.sequence = sequence;
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

            self.sequence = self.getNumber2 ();
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
            //  sequence is a 2-byte integer
            frameSize += 2;
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
            putNumber2 (sequence);
            if (group != null)
                putString (group);
            else
                putNumber1 ((byte) 0);      //  Empty string
            putNumber1 (status);
        
            return msg;
        }

        //  --------------------------------------------------------------------------
        //  Duplicate the HeartBeatMsg message

        public Join dup (Join self)
        {
            if (self == null)
                return null;

            Join copy = new Join ();
            if (self.getIdentity () != null)
                copy.setAddress (self.getIdentity ());
            copy.sequence = self.sequence;
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

            b.append (String.format ("%ssequence=%d\n", tab, (long)sequence));
            if (group != null)
                System.out.printf ("    group='%s'\n", group);
            else
                System.out.printf ("    group=\n");
            b.append (String.format ("%sstatus=%d\n", tab, (long)status));
        }
    }
    
    public static class Leave extends JiniMsg
    {
        private int sequence;
        private String group;
        private int status;

        public Leave ()
        {
            super (LEAVE, HEART_BEAT_MSG_SIGNATURE);
        }

        public Leave (JiniMsg parent)
        {
            super (parent);
        }

        public Leave (
            int sequence,
            String group,
            int status)
        {
            this ();
            this.setSequence (sequence);
            this.setGroup (group);
            this.setStatus (status);
        }

        public void destroy ()
        {
            super.destroy ();
        }

        //  --------------------------------------------------------------------------
        //  Get/set the sequence field

        public int getSequence ()
        {
            return sequence;
        }

        public void setSequence (int sequence)
        {
            this.sequence = sequence;
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

        private static Leave build (JiniMsg parent, Socket input)
        {
            Leave self = new Leave (parent);

            self.sequence = self.getNumber2 ();
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
            //  sequence is a 2-byte integer
            frameSize += 2;
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
            putNumber2 (sequence);
            if (group != null)
                putString (group);
            else
                putNumber1 ((byte) 0);      //  Empty string
            putNumber1 (status);
        
            return msg;
        }

        //  --------------------------------------------------------------------------
        //  Duplicate the HeartBeatMsg message

        public Leave dup (Leave self)
        {
            if (self == null)
                return null;

            Leave copy = new Leave ();
            if (self.getIdentity () != null)
                copy.setAddress (self.getIdentity ());
            copy.sequence = self.sequence;
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

            b.append (String.format ("%sLEAVE:\n", tab0));

            b.append (String.format ("%ssequence=%d\n", tab, (long)sequence));
            if (group != null)
                System.out.printf ("    group='%s'\n", group);
            else
                System.out.printf ("    group=\n");
            b.append (String.format ("%sstatus=%d\n", tab, (long)status));
        }
    }
    
    public static class Ping extends JiniMsg
    {
        private int sequence;

        public Ping ()
        {
            super (PING, HEART_BEAT_MSG_SIGNATURE);
        }

        public Ping (JiniMsg parent)
        {
            super (parent);
        }

        public Ping (
            int sequence)
        {
            this ();
            this.setSequence (sequence);
        }

        public void destroy ()
        {
            super.destroy ();
        }

        //  --------------------------------------------------------------------------
        //  Get/set the sequence field

        public int getSequence ()
        {
            return sequence;
        }

        public void setSequence (int sequence)
        {
            this.sequence = sequence;
        }

        private static Ping build (JiniMsg parent, Socket input)
        {
            Ping self = new Ping (parent);

            self.sequence = self.getNumber2 ();
            
            return self;
        }

        //  --------------------------------------------------------------------------
        //  Get frame size 

        @Override
        protected int getFrameSize ()
        {
            //  Calculate size of serialized data
            int frameSize = 0;
            //  sequence is a 2-byte integer
            frameSize += 2;
            return frameSize;
        }

        //  --------------------------------------------------------------------------
        //  Serialize

        @Override
        protected ZMsg serialize ()
        {
            ZMsg msg = null;
            //  Now serialize message into the frame
            putNumber2 (sequence);
        
            return msg;
        }

        //  --------------------------------------------------------------------------
        //  Duplicate the HeartBeatMsg message

        public Ping dup (Ping self)
        {
            if (self == null)
                return null;

            Ping copy = new Ping ();
            if (self.getIdentity () != null)
                copy.setAddress (self.getIdentity ());
            copy.sequence = self.sequence;
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

            b.append (String.format ("%ssequence=%d\n", tab, (long)sequence));
        }
    }
    
    public static class PingOk extends JiniMsg
    {
        private int sequence;

        public PingOk ()
        {
            super (PING_OK, HEART_BEAT_MSG_SIGNATURE);
        }

        public PingOk (JiniMsg parent)
        {
            super (parent);
        }

        public PingOk (
            int sequence)
        {
            this ();
            this.setSequence (sequence);
        }

        public void destroy ()
        {
            super.destroy ();
        }

        //  --------------------------------------------------------------------------
        //  Get/set the sequence field

        public int getSequence ()
        {
            return sequence;
        }

        public void setSequence (int sequence)
        {
            this.sequence = sequence;
        }

        private static PingOk build (JiniMsg parent, Socket input)
        {
            PingOk self = new PingOk (parent);

            self.sequence = self.getNumber2 ();
            
            return self;
        }

        //  --------------------------------------------------------------------------
        //  Get frame size 

        @Override
        protected int getFrameSize ()
        {
            //  Calculate size of serialized data
            int frameSize = 0;
            //  sequence is a 2-byte integer
            frameSize += 2;
            return frameSize;
        }

        //  --------------------------------------------------------------------------
        //  Serialize

        @Override
        protected ZMsg serialize ()
        {
            ZMsg msg = null;
            //  Now serialize message into the frame
            putNumber2 (sequence);
        
            return msg;
        }

        //  --------------------------------------------------------------------------
        //  Duplicate the HeartBeatMsg message

        public PingOk dup (PingOk self)
        {
            if (self == null)
                return null;

            PingOk copy = new PingOk ();
            if (self.getIdentity () != null)
                copy.setAddress (self.getIdentity ());
            copy.sequence = self.sequence;
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

            b.append (String.format ("%ssequence=%d\n", tab, (long)sequence));
        }
    }
    

    //  --------------------------------------------------------------------------
    //  Receive and parse a HeartBeatMsg from the socket. Returns new object or
    //  null if error. Will block if there's no message waiting.

    public static class HeartBeatMsgBuilder 
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

            case LEAVE:
                self = Leave.build (parent, input);
                break;

            case PING:
                self = Ping.build (parent, input);
                break;

            case PING_OK:
                self = PingOk.build (parent, input);
                break;

            default:
                throw new IllegalArgumentException ();
            }

            return self;
        }
    }

}

