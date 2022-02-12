package jamsesso.meshmap.examples;

import jamsesso.meshmap.*;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class MemoryNode implements MeshMapCluster, AutoCloseable {
    private final Node self;
    private MeshMapServer server;
    private MeshMap map;

    private List<Node> fingers;

    public static final String[] bootstrapNodes = {
            "127.0.0.1#8898#557f12a4-1a6e-4b20-82ca-07ddbc59c6a0", //id 1904384249
//            "127.0.0.1#8898#1b26cbaf-f600-40a2-a37f-7090d50c2405" //id 458612632
    };

    public MemoryNode( Node self ){
        this.self = self;
        this.fingers = new ArrayList<Node>();

        //add bootstrap nodes
        for(String s : bootstrapNodes) {
            if(!s.equals( self.toString() )){
                fingers.add( Node.from(s) );
            }
        }
    }

    @Override
    public List<Node> getAllNodes() {
        return fingers
                .stream()
                .distinct()
                .sorted(Comparator.comparingInt(Node::getId))
                .collect(Collectors.toList());
    }

    @Override
    public <K, V> MeshMap<K, V> join() throws MeshMapException {
        if (this.map != null)
            return (MeshMap<K, V>) this.map;

        //include self
        if(!fingers.contains( self ))
            fingers.add( self );

        server = new MeshMapServer(this, self);
        MeshMapImpl<K, V> map = new MeshMapImpl<>(this, server, self);

        try {
            server.start(map);
            map.open();
        } catch (IOException e) {
            e.printStackTrace();
            throw new MeshMapException("Unable to start the mesh map server");
        }

//        server.broadcast( Message.HI );
        server.broadcast( new Message(Message.TYPE_HI, self) );

        this.map = map;
        return map;
    }

    @Override
    public void close() throws Exception {
        fingers.remove( self );
        if(server != null){
            //TODO: be nice, tell others if you can
            server.broadcast(Message.BYE);
            server.close();
        }
    }
}
