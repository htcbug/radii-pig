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

public class GetMaxBitString extends EvalFunc<Tuple> {

  public Tuple exec(Tuple input) throws IOException {

    if (input == null || input.size() == 0)
      return null;

    try {

      TupleFactory mTupleFactory = TupleFactory.getInstance();
      BagFactory mBagFactory = BagFactory.getInstance();

      // get current bitstring 
      DataBag iterationInfoBag = (DataBag) input.get(0);
      int maxHop = -1;
      String maxBitString = "";
      for (Iterator<Tuple> it = iterationInfoBag.iterator(); it.hasNext();) {
        Tuple iterationInfoTuple = (Tuple) it.next();
        int hop = (Integer) (iterationInfoTuple.get(0));
        String str = (String) (iterationInfoTuple.get(1));
        if (hop > maxHop) {
          maxHop = hop;
          maxBitString = str;
        }
      }
      
      Tuple output = mTupleFactory.newTuple(1);
      output.set(0, maxBitString);

      return output;

    } catch (Exception e) {
      throw new IOException("Caught exception processing input row ", e);
    }
  }
}