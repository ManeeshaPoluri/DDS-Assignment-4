import java.util.List;
import java.io.IOException;
import java.util.Iterator;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.LongWritable;
import com.google.common.collect.Lists;

public class equijoin{

	public static void main(String[] args) throws Exception 
    {
        JobConf configuration = new JobConf(equijoin.class);
        configuration.setJobName("equijoin");
        configuration.setOutputValueClass(Text.class);
        configuration.setOutputKeyClass(Text.class);
        configuration.set("mapred.textoutputformat.separator"," ");
        
        configuration.setReducerClass(Reduce.class);
        configuration.setMapperClass(Map.class);

        FileOutputFormat.setOutputPath(configuration, new Path(args[1]));
        FileInputFormat.setInputPaths(configuration, new Path(args[0]));
        JobClient.runJob(configuration);
    }
    
    public static class Map extends MapReduceBase implements Mapper <LongWritable, Text, Text, Text>
     {

        private Text columns = new Text();
        private Text queries = new Text();
        
        public void map(LongWritable writableKey,Text textVal,OutputCollector<Text,Text> out,Reporter r) throws IOException 
        {

            String pairs[] = textVal.toString().split(",");
            String joinPair = pairs[0];
            String key = pairs[1];
            for (int j=1;j<pairs.length;j++)
                joinPair = joinPair + ","+ pairs[j];

            queries.set(key);
            columns.set(joinPair);
            out.collect(queries,columns);
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer <Text, Text, Text, Text> {
        public void reduce(Text keys, Iterator <Text> keyValue, OutputCollector<Text, Text> out, Reporter r) throws IOException {
        	
        	Text output = new Text();
            boolean b = true;
            String col1 = null;
            
            List<String>  tab1 = new ArrayList<String>();
            List<String>  tab2 = new ArrayList<String>();
        	List<String>  textWrite = new ArrayList<String>();
            
            
            while(keyValue.hasNext()){
                String str = keyValue.next().toString();
                String strval[] = str.split(",");
                if(b == true){
                    b=false;
                    col1 = strval[0];
                    
                }
                if(col1 == strval[0])
                    tab1.add(str);
                else
                    tab2.add(str);
                textWrite.add(str);
            }

            textWrite = Lists.reverse(textWrite);
            if(tab2.size()==0 || tab1.size()==0)
                keys.clear();
            else
            {
                for(int a=0;a<textWrite.size();a++){
                    for(int k=a+1;k<textWrite.size();k++){
                        if(!textWrite.get(a).split(",")[0].equalsIgnoreCase(textWrite.get(k).split(",")[0])){
                            output.set(textWrite.get(a)+" ,"+textWrite.get(k));
                            out.collect(new Text(""), output);
                        }
                    }
                }

            }
        }
    }
}
