package jamsesso.meshmap;


import lombok.Value;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.System.err;
import static java.lang.System.out;

public class MeshMapImpl<K, V>
        implements MeshMap<K, V>, MessageHandler {
    private static final String TYPE_PUT = "PUT";
    private static final String TYPE_GET = "GET";
    private static final String TYPE_REMOVE = "REMOVE";
    private static final String TYPE_CLEAR = "CLEAR";
    private static final String TYPE_KEY_SET = "KEY_SET";
    private static final String TYPE_SIZE = "SIZE";
    private static final String TYPE_CONTAINS_KEY = "CONTAINS_KEY";
    private static final String TYPE_CONTAINS_VALUE = "CONTAINS_VALUE";
    private static final String TYPE_DUMP_ENTRIES = "DUMP_ENTRIES";

    private static final String TYPE_SUCCESSOR = "SUCCESSOR";
    private static final String TYPE_PREDECESSOR = "PREDECESSOR";
    private static final String TYPE_NOTIFY = "NOTIFY";
    private static final String TYPE_PING = "PING";
    private static final String TYPE_FINGERS = "FINGERS";

    private static final String TYPE_JOIN = "JOIN";

    private final CachedMeshMapCluster cluster;
    private final MeshMapServer server;
    private final Node self;
    private final Map<Object, Object> delegate;

    private static Node successor;
    private static Node predecessor;

    private static Timer beat;

    private Queue<Integer> non_response;

    public MeshMapImpl(MeshMapCluster cluster, MeshMapServer server, Node self) {
        this.cluster = new CachedMeshMapCluster(cluster);
        this.server = server;
        this.self = self;
        this.delegate = new ConcurrentHashMap<>();

        this.create();
        this.beat = new Timer();
        non_response = new LinkedList<>();
    }

    @Override
    public Message handle(Message message) {
        switch ( message.getType() ) {
            case Message.TYPE_HI:
                cluster.addNode( message.getPayload(Node.class) );
//                join( message.getPayload(Node.class) );
                return Message.ACK;
//                return new Message(TYPE_JOIN, self);
//                cluster.getAllNodes().forEach(node -> out.println(node.getId() + " @ " + node));
//                return Message.ACK;

//            case Message.TYPE_BYE: {
//                cluster.clearCache();
//                return Message.ACK;
//            }

            case TYPE_GET: {
                //TODO: investigate this
                //node id is incorrect, try ask successor
                Object key = message.getPayload(Object.class);
                return new Message(TYPE_GET, delegate.get(key) );
            }

            case TYPE_PUT: {
                Entry entry = message.getPayload(Entry.class);
                delegate.put(entry.getKey(), entry.getValue());
                return Message.ACK;
            }

            case TYPE_REMOVE: {
                Object key = message.getPayload(Object.class);
                return new Message(TYPE_REMOVE, delegate.remove(key) );
            }

            case TYPE_CLEAR: {
                delegate.clear();
                return Message.ACK;
            }

            case TYPE_KEY_SET: {
                Object[] keys = delegate.keySet().toArray();
                return new Message(TYPE_KEY_SET, keys);
            }

            case TYPE_SIZE: {
                return new Message(TYPE_SIZE, ByteBuffer.allocate(4).putInt(delegate.size()).array());
            }

            case TYPE_CONTAINS_KEY: {
                Object key = message.getPayload(Object.class);
                return delegate.containsKey(key) ? Message.YES : Message.NO;
            }

            case TYPE_CONTAINS_VALUE: {
                Object value = message.getPayload(Object.class);
                return delegate.containsValue(value) ? Message.YES : Message.NO;
            }

            case TYPE_DUMP_ENTRIES: {
                Entry[] entries = delegate.entrySet().stream()
                        .map(entry -> new Entry(entry.getKey(), entry.getValue()))
                        .collect(Collectors.toList())
                        .toArray(new Entry[0]);

                return new Message( TYPE_DUMP_ENTRIES, entries);
            }

            case TYPE_SUCCESSOR: {
                Object key = message.getPayload(Object.class);
                return new Message( TYPE_SUCCESSOR, find_successor( key ) );
            }

            case TYPE_PREDECESSOR: {
                return new Message( TYPE_PREDECESSOR, predecessor );
            }

            case TYPE_NOTIFY: {
                notify( message.getPayload( Node.class ) );
                return Message.ACK;
            }

            case TYPE_PING: {
                return Message.YES;
            }

            case TYPE_FINGERS: {
                Object[] fingers = cluster.getAllNodes()
                        .stream()
                        .filter( node -> node != self )
                        .collect(Collectors.toList())
                        .toArray(new Object[0]);
                return new Message(TYPE_FINGERS, fingers);
            }

            case TYPE_JOIN: {
                join( message.getPayload(Node.class) );
                return Message.ACK;
            }

            default: {
                return Message.ACK;
            }
        }
    }

    private void balance_data(){
        //no need
        if(self == successor)
            return;
        // Ask the successor for their key set.
        Object[] keySet = keySet(successor);

        // Transfer the keys from the successor node that should live on this node.
        List<Object> keysToTransfer = Stream.of(keySet)
                .filter(key -> {
//                    int hash = key.hashCode() & Integer.MAX_VALUE;

//                    if ( self.getId() > successor.getId() ) {
//                        // The successor is the first node (circular node list)
//                        return key.hashCode() <= self.getId() && key.hashCode() > successor.getId();
//                    }

                    return key.hashCode() > self.getId() && key.hashCode() < successor.getId();
                })
                .collect(Collectors.toList());

        // Store the values on the current node.
        keysToTransfer.forEach(key ->
                delegate.put(key, get(key, successor))
        );

        // Delete the keys from the remote node now that the keys are transferred.
        keysToTransfer.forEach(key ->
                remove(key, successor)
        );
    }

    public void request_fingers( Node remote ){
        try{
            Object[] fingers = server.message( remote, new Message(TYPE_FINGERS) ).getPayload(Object[].class);
            Arrays.stream(fingers).collect(Collectors.toList()).forEach(out::println);
            Arrays.stream(fingers).collect(Collectors.toList()).forEach( node -> join((Node)node) );
        }catch(Exception e){

        }
//        try{
//            for (Map.Entry<Node, Message> response : server.broadcast(new Message(TYPE_FINGERS)).entrySet()) {
//                Object[] remoteEntries = response.getValue().getPayload(Object[].class);
//                Arrays.stream(remoteEntries)
//                        .filter( obj -> obj instanceof Node )
//                        .forEach(out::println);
////                        .forEach( node -> join( (Node)node ));
//            }
//        }catch (Exception e){
//            e.printStackTrace();
//        }

//        cluster.getAllNodes().forEach( node -> out.println( node ));
    }

    public void open() throws MeshMapException {
        cluster.getAllNodes()
                .stream()
                .filter( node -> node.getId() != self.getId() )
                .forEach( node -> join( node ) );

        //run house keeping
        beat.schedule(new TimerTask() {
            @Override
            public void run() {
                try {
//                    request_fingers();
                    check_predecessor();
                    stabilize();
                    balance_data();
                    fix_fingers();
                }catch(Exception e ){
//                    err.println("error occurred: " + e.getMessage() );
                }
            }
        },0, 600);
    }

    @Override
    public void close() throws Exception {
        if( successor == self && predecessor == null )
            return;

        if( successor == self )
            delegate.forEach( (key, value) -> put(key, value, predecessor) );

        // Transfer the data from this node to the successor node.
        delegate.forEach( (key, value) -> put(key, value, successor) );

        //stop living
        beat.cancel();
    }

    //http://cit.cs.dixie.edu/cs/3410/asst_chord.html

    /*
        // ask node n to find the successor of id
        n.find_successor(id)
            // Yes, that should be a closing square bracket to match the opening parenthesis.
            // It is a half closed interval.
            if id ∈ (n, successor] then
                return successor
            else
                // forward the query around the circle
                n0 := closest_preceding_node(id)
                return n0.find_successor(id)
    */
    private Node find_successor(Object key) {
        if(successor.equals(self))
            return self;
        //go around the circle; on put hash similarly
        int hash = key.hashCode() & Integer.MAX_VALUE;

        List<Node> fingers = cluster.getAllNodes();

//        return fingers
//                .parallelStream()
//                .filter( node -> node.getId() > )

        return
            cluster.getAllNodes()
                    .stream()
                    .filter( node ->
                            node.getId() > self.getId() && node.getId() <= successor.getId()
                    )
                    .filter( node ->
                            node.getId() >= hash || node.getId() <= hash
                    )
                    .findAny()
                    .orElseGet( () -> {
                        try {
                            return server.message(
                                    closest_preceding_node( hash ),
                                    new Message( TYPE_SUCCESSOR, hash )
                            ).getPayload(Node.class);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        return self;
                    });
    }

    /*
        // search the local table for the highest predecessor of id
        n.closest_preceding_node(id)
            for i = m downto 1 do
                if (finger[i] ∈ (n, id)) then
                    return finger[i]
            return n
    */
    private Node closest_preceding_node(Object key){
        return cluster
                .getAllNodes()
                .stream()
                .filter( node ->
                        node.getId() > self.getId() && node.getId() < key.hashCode()
                )
                .max(Comparator.comparingInt(Node::getId))
                .orElse( self );
    }

    /*
    // create a new Chord ring.
    n.create()
        predecessor := nil
        successor := n
     */
    private void create(){
        predecessor = null;
        successor = self;
    }

    /*
    // join a Chord ring containing node n'.
    n.join(n')
        predecessor := nil
        successor := n'.find_successor(n)
     */
    public void join(Node remote){
        predecessor = null;

        try{
            successor = server.message( remote, new Message( TYPE_SUCCESSOR, self.getId() ) ).getPayload(Node.class);
        }catch(IOException e){}

//        request_fingers( remote );
    }

    /*
        // called periodically. n asks the successor
        // about its predecessor, verifies if n's immediate
        // successor is consistent, and tells the successor about n
        n.stabilize()
            x = successor.predecessor
            if x ∈ (n, successor) then
                successor := x
            successor.notify(n)
    */
    private void stabilize(){
        //id & MAX_VAL??
        if(successor.equals(self))
            return;
        try {
            Node x = server.message( successor, new Message(TYPE_PREDECESSOR) ).getPayload(Node.class);
            if( x.getId() > self.getId() && x.getId() < successor.getId() )
                successor = x;
            server.message(successor, new Message(TYPE_NOTIFY, self ) );
        }catch(IOException e){

        }
    }

    /*
        // n' thinks it might be our predecessor.
        n.notify(n')
            if predecessor is nil or n'∈(predecessor, n) then
                predecessor := n'
     */
    private void notify(Node nPrime){
        if( predecessor == null || nPrime.getId() > predecessor.getId() && nPrime.getId() < self.getId() ){
            predecessor = nPrime;
        }
    }

    /*
        // called periodically. refreshes finger table entries.
        // next stores the index of the finger to fix
        // m = bit in hashkey
        n.fix_fingers()
            next := next + 1
            if next > m then
                next := 1
            finger[next] := find_successor(n+2^{next-1});
    */
    private int next = 1;
    private void fix_fingers() {
        //distance calculation
        int m = Integer.bitCount( Integer.MAX_VALUE );
        next = ( next > m ) ? 1 : next + 1;
        Double idx = next + Math.pow(2, next - 1);

        join( find_successor( idx.intValue() ) );
    }

    /*
        // called periodically. checks whether predecessor has failed.
        n.check_predecessor()
            if predecessor has failed then
                predecessor := nil
    */
    private void check_predecessor(){
        server
                .broadcast( new Message(TYPE_PING) )
                .entrySet()
                .parallelStream()
                .filter( entry -> values().equals(Message.ERR) )
                .map( Map.Entry::getKey )
                .map( node -> {
                    if( node.getId() == predecessor.getId() ){
                        predecessor = null;
                    }
                    non_response.add( node.getId() );
                    cluster.removeNode( node );
                    return null;
                });
    }

    @Override
    public int size() {
        return delegate.size() + server.broadcast( new Message(TYPE_SIZE) ).entrySet().stream()
                .map(Map.Entry::getValue)
                .filter(response -> TYPE_SIZE.equals(response.getType()))
                .mapToInt(Message::getPayloadAsInt)
                .sum();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        Node target = find_successor( key.hashCode() );
        if( target.equals(self) )
            return delegate.containsKey(key);

        Message response;

        try {
            response = server.message(target, new Message(TYPE_CONTAINS_KEY, key) );
        } catch (IOException e) {
            throw new MeshMapRuntimeException(e);
        }

        return Message.YES.equals(response);
    }

    @Override
    public boolean containsValue(Object value) {
        if (delegate.containsValue(value))
            return true;
        return server
                .broadcast( new Message(TYPE_CONTAINS_VALUE, value) )
                .entrySet()
                .stream()
                .map(Map.Entry::getValue)
                .anyMatch(Message.YES::equals);
    }

    @Override
    public V get(Object key) {
        return (V) get(key, find_successor(key));
    }

    @Override
    public V put(K key, V value) {
        put(key, value, find_successor(key));
        return value;
    }

    @Override
    public V remove(Object key) {
        return (V) remove(key, find_successor(key));
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        m.entrySet().parallelStream().forEach(entry -> put(entry.getKey(), entry.getValue()));
    }

    @Override
    public void clear() {
        server.broadcast(new Message(TYPE_CLEAR));
        delegate.clear();
    }

    @Override
    public Set<K> keySet() {
        return cluster.getAllNodes().parallelStream()
                .map(this::keySet)
                .flatMap(Stream::of)
                .map(object -> (K) object)
                .collect(Collectors.toSet());
    }

    @Override
    public Collection<V> values() {
        return entrySet().parallelStream()
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        Message dumpEntriesMsg = new Message(TYPE_DUMP_ENTRIES);
        Set<Map.Entry<K, V>> entries = new HashSet<>();

        for (Map.Entry<Object, Object> localEntry : delegate.entrySet()) {
            entries.add(new TypedEntry<>((K) localEntry.getKey(), (V) localEntry.getValue()));
        }

        for (Map.Entry<Node, Message> response : server.broadcast(dumpEntriesMsg).entrySet()) {
            Entry[] remoteEntries = response.getValue().getPayload(Entry[].class);

            for (Entry remoteEntry : remoteEntries) {
                entries.add(new TypedEntry<>((K) remoteEntry.getKey(), (V) remoteEntry.getValue()));
            }
        }

        return entries;
    }

    public void printLocal() {
        out.println( "MeshMapImpl(Local)[" + String.join(", ", delegate.entrySet().stream()
                .map(entry -> entry.getKey() + ":" + entry.getValue())
                .collect(Collectors.toList()).toArray(new String[0])) + "]\n"
//                "successor: " + successor + " @ " + successor.getId() + "\n" +
//                "predecessor: " + predecessor + " @ " + predecessor.getId()
        );
    }


    //TODO:broadcast?
    private Object get(Object key, Node target) {
        //TODO: check if correct
        if (target.equals(self)) {
            // Value is stored on the local server.
            return delegate.get(key);
        }

        //TODO: check with hashcode
//        int hash = key.hashCode() & Integer.MAX_VALUE;
        Message response;

        try {
            response = server.message( target, new Message(TYPE_GET, key) );
        } catch (IOException e) {
            throw new MeshMapRuntimeException(e);
        }

        if (!TYPE_GET.equals(response.getType())) {
            throw new MeshMapRuntimeException("Unexpected response from remote node: " + response);
        }

        return response.getPayload(Object.class);
    }

    private Object put(Object key, Object value, Node target) {
        if (target.equals(self)) {
            // Value is stored on the local server.
            return delegate.put(key, value);
        }

        Message putMsg = new Message(TYPE_PUT, new Entry(key, value));
        Message response;

        try {
            response = server.message(target, putMsg);
        } catch (IOException e) {
            throw new MeshMapRuntimeException(e);
        }

        if (!Message.ACK.equals(response)) {
            throw new MeshMapRuntimeException("Unexpected response from remote node: " + response);
        }

        return value;
    }

    private Object remove(Object key, Node target) {
        if (target.equals(self)) {
            // Value is stored on the local server.
            return delegate.remove(key);
        }

        Message removeMsg = new Message(TYPE_REMOVE, key);
        Message response;

        try {
            response = server.message(target, removeMsg);
        } catch (IOException e) {
            throw new MeshMapRuntimeException(e);
        }

        if (!TYPE_REMOVE.equals(response.getType())) {
            throw new MeshMapRuntimeException("Unexpected response from remote node: " + response);
        }

        return response.getPayload(Object.class);
    }

    private Object[] nodeList(Node target){
        try{
            return server.message(target, new Message(TYPE_FINGERS))
                    .getPayload(Object[].class);
        }catch(IOException e){

        }
        return null;
    }

    private Object[] keySet(Node target) {
        if (target.equals(self)) {
            // Key is on local server.
            return delegate.keySet().toArray();
        }

        try {
            return server.message(target, new Message(TYPE_KEY_SET) )
                    .getPayload(Object[].class);
        } catch (IOException e) {
            throw new MeshMapRuntimeException(e);
        }
    }

    @Value
    private static class Entry implements Serializable {
        Object key;
        Object value;
    }

    @Value
    private static class TypedEntry<K, V> implements Map.Entry<K, V> {
        K key;
        V value;

        @Override
        public V setValue(V value) {
            throw new UnsupportedOperationException();
        }
    }
}
