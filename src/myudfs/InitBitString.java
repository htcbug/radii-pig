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

public class InitBitString extends EvalFunc<Tuple> {
  public Tuple exec(Tuple input) throws IOException {

    if (input == null || input.size() == 0)
      return null;

    try {
      TupleFactory mTupleFactory = TupleFactory.getInstance();
      BagFactory mBagFactory = BagFactory.getInstance();

      // total number of nodes in the graph, also the length of bitstring
      int nodeNum = (Integer) input.get(0);

      int[] arrayBitString = util.generateBitMask(util.K, nodeNum);
      
      /*
      // store edges' dst nodes
      ArrayList<Tuple> listBitStrings = new ArrayList<Tuple>(util.K);

      // iterate over edges and extract src nodes
      for (int i = 0; i < util.K; i++) {
        Tuple bitStringTuple = mTupleFactory.newTuple(1);
        bitStringTuple.set(0, arrayBitString[i]);
        listBitStrings.add(bitStringTuple);
      } 
      
      Tuple bitStringTuple = mTupleFactory.newTuple(1);
      bitStringTuple.set(0, util.encodeBitMask(arrayBitString, util.K));
      listBitStrings.add(bitStringTuple);
      DataBag bitStringBag = mBagFactory.newDefaultBag(listBitStrings);
      */
      
      Tuple iterationInfo = mTupleFactory.newTuple(2);
      float reachableNodeNum = util.getReachableNodeNum(arrayBitString, util.K);
      // set the hop
      iterationInfo.set(0, 0);
      // set the reachableNodeNum
      iterationInfo.set(1, reachableNodeNum);
      
      DataBag iterationInfoBag = mBagFactory.newDefaultBag();
      iterationInfoBag.add(iterationInfo);
      
      Tuple output = mTupleFactory.newTuple(2);
      
      output.set(0, iterationInfoBag);
      
      //set current bitstring
      output.set(1, util.encodeBitMask(arrayBitString, util.K));
      
      return output;

    } catch (Exception e) {
      throw new IOException("Caught exception processing input row ", e);
    }
  }
}