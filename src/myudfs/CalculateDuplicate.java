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

public class CalculateDuplicate extends EvalFunc<Tuple> {

  public Tuple exec(Tuple input) throws IOException {

    if (input == null || input.size() == 0)
      return null;

    try {

      TupleFactory mTupleFactory = TupleFactory.getInstance();

      LinkedList<Float> listReachableNodeNum = new LinkedList<Float>();

      // get current bitstring
      DataBag iterationInfoBag = (DataBag) input.get(0);
      float maxReachableNodeNum = -1;
      for (Iterator<Tuple> it = iterationInfoBag.iterator(); it.hasNext();) {
        Tuple iterationInfoTuple = (Tuple) it.next();
        float reachableNodeNum = (Float) (iterationInfoTuple.get(1));
        listReachableNodeNum.add(reachableNodeNum);
        if (reachableNodeNum > maxReachableNodeNum) {
          maxReachableNodeNum = reachableNodeNum;
        }
      }
      

      int dumplicateNum = 0;
      for (int i = 0; i < listReachableNodeNum.size(); i++) {
        if (listReachableNodeNum.get(i) == maxReachableNodeNum) {
          dumplicateNum ++;
        }
      }

      Tuple output = mTupleFactory.newTuple(1);
      output.set(0, dumplicateNum);

      return output;

    } catch (Exception e) {
      throw new IOException("Caught exception processing input row ", e);
    }
  }
}