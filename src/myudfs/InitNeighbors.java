package myudfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class InitNeighbors extends EvalFunc<DataBag> {
  public DataBag exec(Tuple input) throws IOException {
    
    if (input == null || input.size() == 0)
      return null;
    
    try {
      TupleFactory mTupleFactory = TupleFactory.getInstance();
      BagFactory mBagFactory = BagFactory.getInstance();

      DataBag values = (DataBag) input.get(0);
      
      // store edges' dst nodes
      LinkedList<Tuple> listSrcNodes = new LinkedList<Tuple>();

      // iterate over edges and extract src nodes
      for (Iterator<Tuple> it = values.iterator(); it.hasNext();) {
        Integer srcNode = (Integer) it.next().get(0);
        Tuple srcNodeTuple = mTupleFactory.newTuple(1);
        srcNodeTuple.set(0, srcNode);
        listSrcNodes.add(srcNodeTuple);
      }
      
      DataBag output = mBagFactory.newDefaultBag(listSrcNodes);
      return output;
      
    } catch (Exception e) {
      throw new IOException("Caught exception processing input row ", e);
    }
  }
}