package org.zyni;

import org.zeromq.ZMsg;

public interface IZyniHandler
{
    void processCommandCallback (Zyni jini, String command, ZMsg msg);
}
