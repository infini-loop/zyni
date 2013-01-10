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

public class TestHeartBeatMsg
{
    @Test
    public void testHeartBeatMsg ()
    {
        System.out.printf (" * heart_beat_msg: ");

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

        HeartBeatMsg.Hello hello = new HeartBeatMsg.Hello ();
        hello.setSequence ((byte) 123);
        hello.appendPeers ("Name: %s", "Brutus");
        hello.appendPeers ("Age: %d", 43);
        hello.insertHeaders ("Name", "Brutus");
        hello.insertHeaders ("Age", "%d", 43);
        hello.send (output);
    
        hello = (HeartBeatMsg.Hello) JiniMsg.recv (input);
        assertNotNull (hello);
        assertEquals (hello.getSequence (), 123);
        assertEquals (hello.getPeers ().size (), 2);
        assertEquals (hello.getPeers ().get (0), "Name: Brutus");
        assertEquals (hello.getPeers ().get (1), "Age: 43");
        assertEquals (hello.getHeaders ().size (), 2);
        assertEquals (hello.getHeadersString ("Name", "?"), "Brutus");
        assertEquals (hello.getHeadersNumber ("Age", 0), 43);
        hello.destroy ();

        HeartBeatMsg.Whisper whisper = new HeartBeatMsg.Whisper ();
        whisper.setSequence ((byte) 123);
        whisper.setContent (new ZFrame ("Captcha Diem"));
        whisper.send (output);
    
        whisper = (HeartBeatMsg.Whisper) JiniMsg.recv (input);
        assertNotNull (whisper);
        assertEquals (whisper.getSequence (), 123);
        assertTrue (whisper.getContent ().streq ("Captcha Diem"));
        whisper.destroy ();

        HeartBeatMsg.Shout shout = new HeartBeatMsg.Shout ();
        shout.setSequence ((byte) 123);
        shout.setGroup ("Life is short but Now lasts for ever");
        shout.setContent (new ZFrame ("Captcha Diem"));
        shout.send (output);
    
        shout = (HeartBeatMsg.Shout) JiniMsg.recv (input);
        assertNotNull (shout);
        assertEquals (shout.getSequence (), 123);
        assertEquals (shout.getGroup (), "Life is short but Now lasts for ever");
        assertTrue (shout.getContent ().streq ("Captcha Diem"));
        shout.destroy ();

        HeartBeatMsg.Join join = new HeartBeatMsg.Join ();
        join.setSequence ((byte) 123);
        join.setGroup ("Life is short but Now lasts for ever");
        join.setStatus ((byte) 123);
        join.send (output);
    
        join = (HeartBeatMsg.Join) JiniMsg.recv (input);
        assertNotNull (join);
        assertEquals (join.getSequence (), 123);
        assertEquals (join.getGroup (), "Life is short but Now lasts for ever");
        assertEquals (join.getStatus (), 123);
        join.destroy ();

        HeartBeatMsg.Leave leave = new HeartBeatMsg.Leave ();
        leave.setSequence ((byte) 123);
        leave.setGroup ("Life is short but Now lasts for ever");
        leave.setStatus ((byte) 123);
        leave.send (output);
    
        leave = (HeartBeatMsg.Leave) JiniMsg.recv (input);
        assertNotNull (leave);
        assertEquals (leave.getSequence (), 123);
        assertEquals (leave.getGroup (), "Life is short but Now lasts for ever");
        assertEquals (leave.getStatus (), 123);
        leave.destroy ();

        HeartBeatMsg.Ping ping = new HeartBeatMsg.Ping ();
        ping.setSequence ((byte) 123);
        ping.send (output);
    
        ping = (HeartBeatMsg.Ping) JiniMsg.recv (input);
        assertNotNull (ping);
        assertEquals (ping.getSequence (), 123);
        ping.destroy ();

        HeartBeatMsg.PingOk pingOk = new HeartBeatMsg.PingOk ();
        pingOk.setSequence ((byte) 123);
        pingOk.send (output);
    
        pingOk = (HeartBeatMsg.PingOk) JiniMsg.recv (input);
        assertNotNull (pingOk);
        assertEquals (pingOk.getSequence (), 123);
        pingOk.destroy ();

        ctx.destroy ();
        System.out.printf ("OK\n");
    }
}
