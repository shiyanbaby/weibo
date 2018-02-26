package weibo;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import util.JobUtil;


import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;

/*
对微博里的单词进行计数统计
* */
public class CommentWordCount {
    public static class ForMapper extends Mapper<LongWritable,Text,Text,NullWritable>{
        private Text okey=new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Map<String,String> map=MRDPUtils.transXmltoMap(value.toString());
            String text=map.get("Text");
            if(text==null){
                return;
            }
            text= StringEscapeUtils.unescapeHtml(text.toLowerCase());
            text = text.replaceAll("'", "");
            text=text.replaceAll("[^a-zA-Z]"," ");//正则表达
            StringTokenizer stringTokenizer=new StringTokenizer(text);
            while(stringTokenizer.hasMoreTokens()){
                okey.set(stringTokenizer.nextToken());
                context.write(okey,NullWritable.get());
            }

        }
    }

    public static class ForReducer extends Reducer<Text,NullWritable,Text,IntWritable>{
        private IntWritable ovalue=new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            int count=0;
            for(NullWritable i:values){
                count++;
            }
            ovalue.set(count);
            context.write(key,ovalue);
        }
    }

    public static void main(String[] args) {
        JobUtil.commitJob(CommentWordCount.class,"D:\\Program Files\\feiq\\Recv Files\\MapReducer所有内容\\MapReduce基础编程 练习题及答案\\MapReduce基础编程 练习题及答案\\MapReduce基础——网站微博数据统计分析\\数据","");
    }
}
