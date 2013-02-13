//  --------------------------------------------------------------------------
//  Selftest

package org.jeromq.jini.heartbeat;

import static org.junit.Assert.*;
import org.junit.Test;
import org.jeromq.ZMQ;
import org.jeromq.ZMQ.Socket;
import org.jeromq.ZFrame;
import org.jeromq.ZContext;
import org.jeromq.jini.JiniMsg;

public class TestGossipMsg
{
    @Test
    public void testGossipMsg ()
    {
        System.out.printf (" * gossip_msg: ");

        //  Create pair of sockets we can send through
        ZContext ctx = new ZContext ();
        assertNotNull (ctx);

        Socket output = ctx.createSocket (ZMQ.DEALER);
        assertNotNull (output);
        output.bind ("inproc://selftest");
        Socket input = ctx.createSocket (ZMQ.ROUTER);
        assertNotNull (input);
        input.connect ("inproc://selftest");
        
        //  Encode/send/decode and verify each message type

        GossipMsg.Hello hello = new GossipMsg.Hello ();
        hello.setIpaddress ("Life is short but Now lasts for ever");
        hello.setMailbox ((byte) 123);
        hello.appendGroups ("Name: %s", "Brutus");
        hello.appendGroups ("Age: %d", 43);
        hello.setStatus ((byte) 123);
        hello.insertHeaders ("Name", 200, "Brutus");
        hello.insertHeaders ("Age", 100, "%d", 43);
        hello.send (output);
    
        hello = (GossipMsg.Hello) JiniMsg.recv (input);
        assertNotNull (hello);
        assertEquals (hello.getIpaddress (), "Life is short but Now lasts for ever");
        assertEquals (hello.getMailbox (), 123);
        assertEquals (hello.getGroups ().size (), 2);
        assertEquals (hello.getGroups ().get (0), "Name: Brutus");
        assertEquals (hello.getGroups ().get (1), "Age: 43");
        assertEquals (hello.getStatus (), 123);
        assertEquals (hello.getHeaders ().size (), 2);
        assertEquals (hello.getHeaders ().get ("Name").getVersion (), 200);
        assertEquals (hello.getHeaders ().get ("Age").getVersion (), 100);
        assertEquals (hello.getHeadersString ("Name", "?"), "Brutus");
        assertEquals (hello.getHeadersNumber ("Age", 0), 43);
        hello.destroy ();

        GossipMsg.Whisper whisper = new GossipMsg.Whisper ();
        whisper.setContent (new ZFrame ("Captcha Diem"));
        whisper.send (output);
    
        whisper = (GossipMsg.Whisper) JiniMsg.recv (input);
        assertNotNull (whisper);
        assertTrue (whisper.getContent ().streq ("Captcha Diem"));
        whisper.destroy ();

        GossipMsg.Shout shout = new GossipMsg.Shout ();
        shout.setGroup ("Life is short but Now lasts for ever");
        shout.setContent (new ZFrame ("Captcha Diem"));
        shout.send (output);
    
        shout = (GossipMsg.Shout) JiniMsg.recv (input);
        assertNotNull (shout);
        assertEquals (shout.getGroup (), "Life is short but Now lasts for ever");
        assertTrue (shout.getContent ().streq ("Captcha Diem"));
        shout.destroy ();

        GossipMsg.Join join = new GossipMsg.Join ();
        join.setGroup ("Life is short but Now lasts for ever");
        join.setStatus ((byte) 123);
        join.send (output);
    
        join = (GossipMsg.Join) JiniMsg.recv (input);
        assertNotNull (join);
        assertEquals (join.getGroup (), "Life is short but Now lasts for ever");
        assertEquals (join.getStatus (), 123);
        join.destroy ();

        GossipMsg.State state = new GossipMsg.State ();
        state.setGeneration ((byte) 123);
        state.setMaxVersion ((byte) 123);
        state.insertExtra ("Name", 200, "Brutus");
        state.insertExtra ("Age", 100, "%d", 43);
        state.send (output);
    
        state = (GossipMsg.State) JiniMsg.recv (input);
        assertNotNull (state);
        assertEquals (state.getGeneration (), 123);
        assertEquals (state.getMaxVersion (), 123);
        assertEquals (state.getExtra ().size (), 2);
        assertEquals (state.getExtra ().get ("Name").getVersion (), 200);
        assertEquals (state.getExtra ().get ("Age").getVersion (), 100);
        assertEquals (state.getExtraString ("Name", "?"), "Brutus");
        assertEquals (state.getExtraNumber ("Age", 0), 43);
        state.destroy ();

        GossipMsg.Ping ping = new GossipMsg.Ping ();
        ping.send (output);
    
        ping = (GossipMsg.Ping) JiniMsg.recv (input);
        assertNotNull (ping);
        ping.destroy ();

        GossipMsg.PingOk pingOk = new GossipMsg.PingOk ();
        pingOk.send (output);
    
        pingOk = (GossipMsg.PingOk) JiniMsg.recv (input);
        assertNotNull (pingOk);
        pingOk.destroy ();

        GossipMsg.PingEnd pingEnd = new GossipMsg.PingEnd ();
        pingEnd.send (output);
    
        pingEnd = (GossipMsg.PingEnd) JiniMsg.recv (input);
        assertNotNull (pingEnd);
        pingEnd.destroy ();

        GossipMsg.Exit exit = new GossipMsg.Exit ();
        exit.send (output);
    
        exit = (GossipMsg.Exit) JiniMsg.recv (input);
        assertNotNull (exit);
        exit.destroy ();

        ctx.destroy ();
        System.out.printf ("OK\n");
    }
}
