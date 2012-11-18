package myudfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class MergeReachableNodes extends EvalFunc<Tuple> {

  public Tuple exec(Tuple input) throws IOException {

    if (input == null || input.size() == 0)
      return null;

    try {

      TupleFactory mTupleFactory = TupleFactory.getInstance();
      BagFactory mBagFactory = BagFactory.getInstance();

      HashSet<Tuple> setReachableNodes = new HashSet<Tuple>();
      
      // get reachable_node_nums 
      DataBag reachableNodeNums = (DataBag) input.get(0);
      int maxHop = 0;
      for (Iterator<Tuple> it = reachableNodeNums.iterator(); it.hasNext();) {
        int hop = (Integer) (((Tuple) it.next()).get(0));
        if (hop > maxHop)
          maxHop = hop;
      }

      // add old reachable_nodes first
      DataBag oldNodes = (DataBag) input.get(1);
      for (Iterator<Tuple> it = oldNodes.iterator(); it.hasNext();) {
        setReachableNodes.add(it.next());
      }
      
      int oldNodesNum = setReachableNodes.size();

      DataBag values = (DataBag) input.get(2);

      // add new reachable_nodes 
      for (Iterator<Tuple> it = values.iterator(); it.hasNext();) {
        DataBag newNodes = (DataBag) (((Tuple) it.next()).get(1));
        for (Iterator<Tuple> node_it = ((DataBag) newNodes).iterator(); node_it.hasNext();) {
          setReachableNodes.add(node_it.next());
        }
      }
      
      int foundNewReachableNode = 0;
      if (setReachableNodes.size() != oldNodesNum)
        foundNewReachableNode = 1;

      
      DataBag reachableNodes = mBagFactory.newDefaultBag(new ArrayList<Tuple>(setReachableNodes));
     
      // add new hop to reachable_node_nums
      Tuple newReachableNodeNum = mTupleFactory.newTuple(2);
      newReachableNodeNum.set(0, maxHop+1);
      newReachableNodeNum.set(1, setReachableNodes.size());
      reachableNodeNums.add(newReachableNodeNum);

      Tuple output = mTupleFactory.newTuple(3);
      output.set(0, reachableNodeNums);
      output.set(1, reachableNodes);
      output.set(2, foundNewReachableNode);

      return output;

    } catch (Exception e) {
      throw new IOException("Caught exception processing input row ", e);
    }
  }
}