package myudfs;

//Bit Shuffle Encoder/Decoder
class BitShuffleCoder {
  // decode bitstrings
  public static int[] decode_bitmasks(String str, int K) {
    int i, j;
    int[] result = new int[K];

    int cur_value;
    int fill_value = 1;
    int cumulated_value = 0;

    for (i = 0; i < K; i++)
      result[i] = 0;

    int[] byte_buffer = new int[4];
    int byte_bufpos = 0;
    int cur_byte;
    byte[] str_bytes = str.getBytes();

    for (i = 0; i < str_bytes.length; i += 2) {
      cur_byte = Integer.parseInt(str.substring(i, i + 2), 16);

      if ((cur_byte & 0x80) != 0) {
        byte_buffer[byte_bufpos++] = cur_byte & 0x7F;

        cur_value = 0;
        for (j = 0; j < byte_bufpos; j++) {
          cur_value += (byte_buffer[j] << (7 * (byte_bufpos - 1 - j)));
        }

        // fill only one
        if (fill_value == 1 && cur_value > 0)
          fill_result(result, K, cur_value, cumulated_value);

        cumulated_value += cur_value;
        fill_value = 1 - fill_value;
        byte_bufpos = 0;
      } else {
        byte_buffer[byte_bufpos++] = cur_byte & 0x7F;
      }
    }

    return result;
  }

  private static void fill_result(int[] result, int K, int cur_value, int cumulated_value) {
    int i, j;
    int start_i = cumulated_value / K; // i : bit position of each bitmask
    int start_j = cumulated_value % K; // j : index of bitmask
    int count = 0;

    for (i = start_i; i < 32; i++) {
      if (i == start_i)
        j = start_j;
      else
        j = 0;

      for (; j < K; j++) {
        result[j] |= (1 << (31 - i));
        if (++count >= cur_value)
          return;
      }
    }

    return;
  }

  // encode bitmask
  public static String encode_bitmasks(int[] bm_array, int K) {
    String result = "";
    int i, j;
    byte prev_bit = -1;
    int cur_count = 0;
    byte cur_bit;
    int cur_mask;

    for (i = 0; i < 32; i++) { // i : bit position of each bitmask
      cur_mask = 1 << (31 - i);
      for (j = 0; j < K; j++) { // j : index of bitmask
        if ((cur_mask & bm_array[j]) != 0)
          cur_bit = 1;
        else
          cur_bit = 0;

        if (prev_bit == -1) {
          if (cur_bit == 0)
            result += encode_value(0); // bit sequence start with 1.

          prev_bit = cur_bit;
          cur_count = 1;
          continue;
        }

        if (prev_bit == cur_bit) {
          cur_count++;
        } else {
          result += encode_value(cur_count);

          prev_bit = cur_bit;
          cur_count = 1;
        }
      }
    }

    if (cur_count > 0) {
      result += encode_value(cur_count);
    }

    return result;
  }

  // encode bitmask
  public static String encode_bitmasks(long[] bm_array, int K) {
    String result = "";
    int i, j;
    byte prev_bit = -1;
    int cur_count = 0;
    byte cur_bit;
    long cur_mask;

    for (i = 0; i < 32; i++) { // i : bit position of each bitmask
      cur_mask = 1 << (31 - i);
      for (j = 0; j < K; j++) { // j : index of bitmask
        if ((cur_mask & bm_array[j]) != 0)
          cur_bit = 1;
        else
          cur_bit = 0;

        if (prev_bit == -1) {
          if (cur_bit == 0)
            result += encode_value(0); // bit sequence start with 1.

          prev_bit = cur_bit;
          cur_count = 1;
          continue;
        }

        if (prev_bit == cur_bit) {
          cur_count++;
        } else { // prev_bit != cur_bit
          result += encode_value(cur_count);

          prev_bit = cur_bit;
          cur_count = 1;
        }
      }
    }

    if (cur_count > 0) {
      result += encode_value(cur_count);
    }

    return result;
  }

  private static String encode_value(int number) {
    if (number == 0) {
      return "80";
    }

    // find leftmost bit
    int i;
    int cur_mask;
    int result = 0;
    final int[] one_masks = { 0x7F, 0x3F80, 0x1FC000, 0xFE00000, 0xF0000000 };

    for (i = 31; i >= 0; i--) {
      cur_mask = 1 << i;
      if ((cur_mask & number) != 0)
        break;
    }

    int nbytes = (int) Math.ceil((float) (i + 1) / 7.0);

    for (i = 0; i < nbytes; i++) {
      if (i == 0) {
        result = (1 << 7) | (number & one_masks[0]);
      } else {
        int added_value = ((number & (one_masks[i]))) >> (7 * i);
        result |= (added_value << (8 * i));
      }
    }

    String temp_result = Integer.toHexString(result);
    if (temp_result.length() % 2 == 1)
      temp_result = "0" + temp_result;

    return temp_result;
  }

};

// Flajolet - Martin bitmask(bitstring) class
class FMBitmask {
  // generate K replicated bitmasks for one node
  public static String generate_bitmask(int number_node, int K, int encode_bitmask) {
    int i;
    int size_bitmask = 32;
    String bitmask = "bsi0:0:1";
    int bm_array[] = new int[K];

    for (i = 0; i < K; i++) {
      if (encode_bitmask == 1)
        bm_array[i] = create_random_bm(number_node, size_bitmask);
      else
        bitmask = bitmask + " " + Integer.toHexString(create_random_bm(number_node, size_bitmask));
    }

    if (encode_bitmask == 1) {
      String encoded_bitmask = BitShuffleCoder.encode_bitmasks(bm_array, K);

      bitmask += (" " + encoded_bitmask);
    }

    return bitmask;
  }

  // Create a Flajolet-Martin bitstring. The maximum number of nodes is currently 4 billion.
  public static int create_random_bm(int number_node, int size_bitmask) {
    int j;

    // cur_random is between 0 and 1.
    double cur_random = Math.random();

    double threshold = 0;
    for (j = 0; j < size_bitmask - 1; j++) {
      threshold += Math.pow(2, -1 * j - 1);

      if (cur_random < threshold) {
        break;
      }
    }

    int bitmask = 0;

    if (j < size_bitmask - 1) {
      int small_bitmask = 1 << (size_bitmask - 1 - j);
      // move small_bitmask to MSB bits of bitmask;
      bitmask = small_bitmask << (32 - size_bitmask);
    }

    return bitmask;
  }

  // calculate Neighborhood function N(h) from bitmasks.
  public static double nh_from_bitmask(int[] bitmask, int K) {
    int i;
    double avg_bitpos = 0;

    for (i = 0; i < K; i++) {
      avg_bitpos += (double) FMBitmask.find_least_zero_pos(bitmask[i]);
    }

    avg_bitpos = avg_bitpos / (double) K;

    return Math.pow(2, avg_bitpos) / 0.77351;
  }

  // fine the least zero bit position in a number
  public static int find_least_zero_pos(int number) {
    int i;

    for (i = 0; i < 32; i++) {
      int mask = 1 << (31 - i);

      if ((number & mask) == 0) {
        return i;
      }
    }

    return i;
  }
}

public class util {

  // number of Flajolet-Martin bitstrings for a node
  public static int K = 32;

  public static float Threshold = 0.9f;

  // create a Flajolet-Martin bitstring.
  // the maximum number of nodes is currently 4 billion.
  public static int generateRandomBitString(int size_bitmask) {
    int j = 0;
    double cur_random = 0;
    double threshold = 0;

    cur_random = Math.random();
    for (j = 0; j < size_bitmask - 1; j++) {
      threshold += Math.pow(2, -1 * j - 1);

      if (cur_random < threshold) {
        break;
      }
    }

    int bitmask = 0;
    if (j < size_bitmask - 1) {
      int small_bitmask = 1 << (size_bitmask - 1 - j);
      // move small_bitmask to MSB bits of bitmask;
      bitmask = small_bitmask << (32 - size_bitmask);
    }

    return bitmask;
  }

  public static int[] generateBitMask(int size_bitmask, int K) {
    int i;
    int bm_array[] = new int[K];

    for (i = 0; i < K; i++) {
      bm_array[i] = generateRandomBitString(size_bitmask);
    }

    return bm_array;
  }

  public static String encodeBitMask(int[] bitMaskArrary, int K) {
    return BitShuffleCoder.encode_bitmasks(bitMaskArrary, K);
  }

  public static int[] decodeBitMask(String str, int K) {

    return BitShuffleCoder.decode_bitmasks(str, K);

  }

  public static float getReachableNodeNum(int[] bitMaskArray, int K) {

    return (float) FMBitmask.nh_from_bitmask(bitMaskArray, K);
  }

  public static void main(String args[]) {

    int[] t = util.generateBitMask(32, 10);
    String en = util.encodeBitMask(t, 10);

    int[] de = util.decodeBitMask(en, 10);

    for (int i = 0; i < 10; i++)
      System.out.println(de[i] + " VS " + t[i]);
  }

}
