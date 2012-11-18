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

public class MergeBitString extends EvalFunc<Tuple> {

  public Tuple exec(Tuple input) throws IOException {

    if (input == null || input.size() == 0)
      return null;

    try {

      TupleFactory mTupleFactory = TupleFactory.getInstance();
      BagFactory mBagFactory = BagFactory.getInstance();

      // get current bitstring 
      DataBag iterationInfoBag = (DataBag) input.get(0);
      int maxHop = -1;
      for (Iterator<Tuple> it = iterationInfoBag.iterator(); it.hasNext();) {
        Tuple iterationInfoTuple = (Tuple) it.next();
        int hop = (Integer) (iterationInfoTuple.get(0));
        if (hop > maxHop) {
          maxHop = hop;
        }
      }
      
      // current bit string array
      String maxBitString = (String) input.get(1);
      int[] arrayBitString = util.decodeBitMask(maxBitString, util.K);

      DataBag neighborBitStringBag = (DataBag) input.get(2);
      
      // iterate over neighbor bitstrings and OR with current bitstring
      for (Iterator<Tuple> it = neighborBitStringBag.iterator(); it.hasNext();) {
        String neighborBitString = (String) (((Tuple) it.next()).get(1));
        int[] arrayNeighborBitString = util.decodeBitMask(neighborBitString, util.K);

        for (int i = 0; i < util.K; i++) {
          arrayBitString[i] = (arrayBitString[i] | arrayNeighborBitString[i]);
        }
      }

      String newMaxBitString = util.encodeBitMask(arrayBitString, util.K);
      int bitStringUpdated = 1;
      if ( maxBitString.equals(newMaxBitString))
        bitStringUpdated = 0;

      // add new hop to bitStringBag
      Tuple iterationInfo = mTupleFactory.newTuple(2);
      // set the hop
      iterationInfo.set(0, maxHop+1);
      // set the bit strings
      float reachableNodeNum = util.getReachableNodeNum(arrayBitString, util.K);
      iterationInfo.set(1, reachableNodeNum);
     
      iterationInfoBag.add(iterationInfo);
      
      Tuple output = mTupleFactory.newTuple(3);
      output.set(0, iterationInfoBag);
      output.set(1, newMaxBitString);
      output.set(2, bitStringUpdated);

      return output;

    } catch (Exception e) {
      throw new IOException("Caught exception processing input row ", e);
    }
  }
}