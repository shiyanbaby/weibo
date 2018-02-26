package weibo;
/**
 * 求最大值和最小值
 * 在网站的数据统计中，有这样一种情况:
 * 即统计某个用户发表的评论数、第一次发表评论的时间(最小日期)和最后一次发表评论的时间（最大日期）。
 * 下面代码就是解决input/comments.xml的这个问题
 */
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import util.JobUtil;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public class MaxMinSimple{

    public static class ForMapper extends Mapper<LongWritable,Text,Text,MaxMinTime>{
        private Text okey=new Text();
        private MaxMinTime ovalue=new MaxMinTime();
        private static final SimpleDateFormat frmt=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Map<String,String> map=MRDPUtils.transXmltoMap(value.toString());
            String id=map.get("Id");
            String time=map.get("CreationDate");
            if(id==null||time==null){
                return;
            }

            try{
                  Date creationDate =frmt.parse(time);
                  ovalue.setMax(creationDate);
                  ovalue.setMin(creationDate);
                  ovalue.setCount(1);
                  okey.set(id);
                  context.write(okey,ovalue);
            } catch (ParseException e) {
                e.printStackTrace();
            }

        }
    }

    public static class ForReducer extends Reducer<Text,MaxMinTime,Text,MaxMinTime>{
          private  MaxMinTime ovalue=new MaxMinTime();

        @Override
        protected void reduce(Text key, Iterable<MaxMinTime> values, Context context) throws IOException, InterruptedException {
            int sum=0;
            for(MaxMinTime i:values){
                if(ovalue.getMin()==null||ovalue.getMin().compareTo(i.getMin())<0){
                        ovalue.setMin(i.getMin());
                }

                if(ovalue.getMax()==null||ovalue.getMax().compareTo(i.getMax())>0){
                    ovalue.setMax(i.getMax());
                }
                sum+=i.getCount();

            }
             ovalue.setCount(sum);
            context.write(key,ovalue);
        }
    }

    public static void main(String[] args) {
        JobUtil.commitJob(MaxMinSimple.class,"D:\\Program Files\\feiq\\Recv Files\\MapReducer所有内容\\MapReduce基础编程 练习题及答案\\MapReduce基础编程 练习题及答案\\MapReduce基础——网站微博数据统计分析\\数据","");
    }
}
