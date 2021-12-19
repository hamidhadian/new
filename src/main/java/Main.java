import java.util.concurrent.TimeUnit;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.LocalCluster;

public class Main {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        SentenceSpout sentenceGenerator = new SentenceSpout();
        //SlowSentenceGeneratorSpout slowSentenceGeneratorSpout = new SlowSentenceGeneratorSpout();

        SplitSentenceBolt splitter = new SplitSentenceBolt();
        WordCountBolt counter = new WordCountBolt();
        //SemiComplexBolt square = new SemiComplexBolt();
       // ComplexBolt fact = new ComplexBolt();
        LogWriter logWriter = new LogWriter();


        //Heavy branch
        builder.setSpout("sentenceGenerator", sentenceGenerator);
        builder.setBolt("splitter", splitter).shuffleGrouping("sentenceGenerator");
        builder.setBolt("counter", counter).shuffleGrouping("splitter");
       // builder.setBolt("square", square, 2).shuffleGrouping("counter");
        //builder.setBolt("fact", fact, 3).shuffleGrouping("counter");

        BoltDeclarer bd = builder.setBolt("logWriter", logWriter).shuffleGrouping("counter");
       // BoltDeclarer bd = builder.setBolt("logWriter", logWriter, 5);
       // bd.allGrouping(("square"));
       // bd.allGrouping(("fact"));




        Config config = new Config();
        config.setDebug(true);

        //remote
        config.put("dirToWrite", "/home/ali/work/");
        //local
//        config.put("dirToWrite", "E:\\Elmosi\\982\\");


        //for local
//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology("mainTopology", config, builder.createTopology());
//        TimeUnit.SECONDS.wait(10);
//        cluster.killTopology("mainTopology");
//        cluster.shutdown();

        // for remote
        try{
            StormSubmitter.submitTopology("MyTopology", config, builder.createTopology());
        }catch (Exception e){
            e.printStackTrace();
        }



    }
}