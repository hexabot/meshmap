package jamsesso.meshmap;

import java.util.List;

public class CachedMeshMapCluster implements MeshMapCluster {
  private final Object[] lock = new Object[0];
  private final MeshMapCluster delegate;
  private List<Node> nodes;

  public CachedMeshMapCluster(MeshMapCluster cluster) {
    this.delegate = cluster;
  }

  @Override
  public List<Node> getAllNodes() {
    synchronized (lock) {
      if(nodes == null) {
        nodes = delegate.getAllNodes();
      }
      return nodes;
    }
  }

  @Override
  public <K, V> MeshMap<K, V> join() throws MeshMapException {
    return delegate.join();
  }

  //nope, fix_fingers...
  public void clearCache() {
    synchronized (lock) {
      nodes = null;
      //TODO: foreach n.stabilize
    }
  }

  public void addNode(Node n){
    synchronized (lock){
      if(nodes == null){
        nodes = delegate.getAllNodes();
      }
      if( !nodes.contains(n) && n != null )
        nodes.add( n );
    }
  }

  public void removeNode(Node n){
    synchronized (lock){
      if(nodes == null){
        nodes = delegate.getAllNodes();
      }
      if(nodes.contains(n))
        nodes.remove( n );
    }
  }
}
