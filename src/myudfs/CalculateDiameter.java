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

public class CalculateDiameter extends EvalFunc<Tuple> {

  public Tuple exec(Tuple input) throws IOException {

    if (input == null || input.size() == 0)
      return null;

    try {

      TupleFactory mTupleFactory = TupleFactory.getInstance();

      LinkedList<Double> listReachableNodeNum = new LinkedList<Double>();

      // get current bitstring
      DataBag iterationInfoBag = (DataBag) input.get(0);
      double maxReachableNodeNum = -1;
      for (Iterator<Tuple> it = iterationInfoBag.iterator(); it.hasNext();) {
        Tuple iterationInfoTuple = (Tuple) it.next();
        double reachableNodeNum = (Double) (iterationInfoTuple.get(1));
        listReachableNodeNum.add(reachableNodeNum);
        if (reachableNodeNum > maxReachableNodeNum) {
          maxReachableNodeNum = reachableNodeNum;
        }
      }

      int largeDiameter = 0;
      int smallDiameter = 0;
      for (int i = 0; i < listReachableNodeNum.size(); i++) {
        if (listReachableNodeNum.get(i) > util.Threshold * maxReachableNodeNum) {
          largeDiameter = i;
          if (i > 0)
            smallDiameter = i - 1;
          break;
        }
      }
      
      double smallNum = listReachableNodeNum.get(smallDiameter);
      double largeNum = listReachableNodeNum.get(largeDiameter);

      double diameter = smallDiameter + (0.9*maxReachableNodeNum-smallNum)/(largeNum-smallNum);
      
      Tuple output = mTupleFactory.newTuple(1);
      output.set(0, diameter);

      return output;

    } catch (Exception e) {
      throw new IOException("Caught exception processing input row ", e);
    }
  }
}