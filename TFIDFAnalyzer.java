import com.huaban.analysis.jieba.JiebaSegmenter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.*;

public class TFIDFAnalyzer {

    private HashMap<String,Double> idfMap = null;
    private HashSet<String> stopWordsSet = null;
    private double idfMedian = 1.0;

    public TFIDFAnalyzer() {
        if(stopWordsSet==null) {
            stopWordsSet=new HashSet<>();
            loadStopWords("./dict/stop_words.txt");
        }
        if(idfMap==null) {
            idfMap=new HashMap<>();
            loadIDFMap("./dict/idf.txt");
        }

    }

    private void loadStopWords(String filePath) {
        try {
            File file = new File(filePath);
            if(file.isFile() && file.exists()) {
                InputStreamReader read = new InputStreamReader(
                        new FileInputStream(file),"UTF8");
                BufferedReader bufferedReader = new BufferedReader(read);
                String line = null;
                while((line = bufferedReader.readLine()) != null){
                    stopWordsSet.add(line.trim());
                }
                read.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void loadIDFMap(String filePath) {
        try {
            File file = new File(filePath);
            if(file.isFile() && file.exists()) {
                InputStreamReader read = new InputStreamReader(
                        new FileInputStream(file),"UTF8");
                BufferedReader bufferedReader = new BufferedReader(read);
                String line = null;
                while((line = bufferedReader.readLine()) != null) {
                    String[] kv=line.trim().split(" ");
                    idfMap.put(kv[0],Double.parseDouble(kv[1]));
                }
                read.close();
                List<Double> idfList=new ArrayList<>(idfMap.values());
                Collections.sort(idfList);
                idfMedian=idfList.get(idfList.size()/2);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public List<Keyword> analyze(String content, int topN) {
        List<Keyword> keywordList=new ArrayList<>();

        Map<String, Double> tfMap=getTF(content);
        for(String word:tfMap.keySet()) {
            if(idfMap.containsKey(word)) {
                keywordList.add(new Keyword(word,idfMap.get(word)*tfMap.get(word)));
            } else {
                keywordList.add(new Keyword(word, idfMedian * tfMap.get(word)));
            }
        }
        if (keywordList.size()==0) {
            return keywordList;
        }

        Collections.sort(keywordList);

        if(keywordList.size()>topN) {
            int num=keywordList.size()-topN;
            for(int i=0;i<num;i++) {
                keywordList.remove(topN);
            }
        }
        return keywordList;
    }

    public Map<String, Double> getTF(String content) {
        Map<String,Double> tfMap=new HashMap<>();
        if(content==null || content.equals(""))
            return tfMap;

        JiebaSegmenter segmenter = new JiebaSegmenter();
        List<String> segments = segmenter.sentenceProcess(content);
        Map<String,Integer> freqMap=new HashMap<>();

        int wordSum=0;
        for(String segment:segments) {
            segment = segment.trim();
            if(!stopWordsSet.contains(segment) && segment.length()>1) {
                wordSum++;
                if(freqMap.containsKey(segment)) {
                    freqMap.put(segment,freqMap.get(segment)+1);
                }else {
                    freqMap.put(segment, 1);
                }
            }
        }

        for(String word:freqMap.keySet()) {
            tfMap.put(word,freqMap.get(word)*0.1/wordSum);
        }

        return tfMap;
    }
}
