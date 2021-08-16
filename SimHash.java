import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.List;


public class SimHash {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private TFIDFAnalyzer tfidfAnalyzer = null;

    public SimHash() {
        tfidfAnalyzer = new TFIDFAnalyzer();
    }

    public BigInteger hash(String source) {
        if (source == null || source.length() == 0) {
            return new BigInteger("0");
        } else {
            char[] sourceArray = source.toCharArray();
            BigInteger x = BigInteger.valueOf(((long) sourceArray[0]) << 7);
            BigInteger m = new BigInteger("1000003");
            BigInteger mask = new BigInteger("2").pow(128).subtract(new BigInteger("1"));
            for (char item : sourceArray ) {
                BigInteger temp = BigInteger.valueOf((long) item);
                x = x.multiply(m).xor(temp).and(mask);
            }
            x = x.xor(new BigInteger(String.valueOf(source.length())));
            if (x.equals(new BigInteger("-1"))) {
                x = new BigInteger("-2");
            }
            return x;
        }
    }

    public String getSimhash(String content) {
        List<Keyword> words =  tfidfAnalyzer.analyze(content, 6);
        if (words.size()==0) {
            return "00";
        }
        int[] v = new int[64];
        for (Keyword w:words) {
            Double d = w.getTfidfvalue()*200;
            int weight = d.intValue();
            BigInteger t = hash(w.getName());
            for (int i = 0; i < 64; i++) {
                BigInteger bitmask = new BigInteger("1").shiftLeft(i);
                if (t.and(bitmask).signum() != 0) {
                    v[i] += weight;
                } else {
                    v[i] -= weight;
                }
            }
        }
        String res = "";
        for (int i = 63; i >= 0; i--) {
            if (v[i] >= 0) {
                res += "1";
            } else {
                res += "0";
            }
        }
       return res;
    }

    public int getHammDist(String simhash1, String simhash2) {
        int distance;
        if (simhash1.length() != simhash2.length()) {
            distance = 64;
        } else {
            distance = 0;
            for (int i = 0; i < simhash1.length(); i++) {
                if (simhash1.charAt(i) != simhash2.charAt(i)) {
                    distance++;
                }
            }
        }
        return distance;
    }
}
