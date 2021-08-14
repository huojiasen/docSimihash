import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import simhash.SimHash;
import java.util.*;

public class updateCenter {
    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("dump.updateCenter");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        String inputFile =  "";
        // 1 read from mongodb
        JavaRDD<String> lines = ctx.textFile(inputFile, 1);
        JavaRDD<DocEntity> docs = lines.map(l->new DocEntity());

        // 2 process
        JavaRDD<ClusterEntity> clusters = docs.filter(d->{
            int i = d.getTimeStamp().compareTo(new Date( 2021, 1, 1));
            if (i==1) {
                return true;
            } else {
                return false;
            }
        }).mapToPair(d->{
            ArrayList<DocEntity> r = new ArrayList<>();
            r.add(d);
            return new Tuple2<String, ArrayList<DocEntity>>(d.getClusterId(),r);
        }).reduceByKey((Doc1,Doc2)->{
            Doc1.addAll(Doc2);
            return Doc1;
        }).map((cluster)->{
            String clusterId  = cluster._1;
            ArrayList<DocEntity>  DocList = cluster._2;
            SimHash sh = new SimHash();
            int minMean = Integer.MAX_VALUE;
            int minIndex = 0;
            ClusterEntity ce = new ClusterEntity();
            for(int i=0;i<DocList.size();i++) {
                int sumDist = 0;
                for (int j=0;j<DocList.size();j++) {
                    sumDist += sh.getHammDist(DocList.get(i).getContentSimhash(), DocList.get(j).getContentSimhash());
                }
                if(sumDist<minMean) {
                    minMean = sumDist;
                    minIndex = i;
                }
            }
            DocEntity res = DocList.get(minIndex);
            ce.setId(clusterId);
            ce.setCenter(res.getContentSimhash());
            List<ClusterDocument> temp = new ArrayList<>();
            // save doc near the center
            for(int i=0;i<DocList.size();i++) {
                if (sh.getHammDist(DocList.get(i).getContentSimhash(),res.getContentSimhash())<10) {
                    ClusterDocument d = new ClusterDocument();
                    d.setDocId(DocList.get(i).getDocId());
                    d.setContentSimhash(DocList.get(i).getContentSimhash());
                    d.setTitleSimhash(DocList.get(i).getTitleSimhash());
                    temp.add(d);
                }
            }
            ce.setDocs(temp);
            return ce;
        });
        // 3 write
        ctx.stop();
    }
}
