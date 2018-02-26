package weibo;

import org.apache.hadoop.io.IntWritable;
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


public class AverageDriver {
    public static class ForMapper extends Mapper<LongWritable,Text,IntWritable,AvgCount>{
        private IntWritable okey=new IntWritable();
        private AvgCount ovalue =new AvgCount();
          private static final SimpleDateFormat frmt= new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Map<String,String> map=MRDPUtils.transXmltoMap(value.toString());
            String time=map.get("CreationDate");
            String text=map.get("Text");
            if(time==null||text==null){
                return;
            }
            try {
                Date creationDate=frmt.parse(time);
                okey.set(creationDate.getHours());
                ovalue.setCount(1);
                ovalue.setAvg(text.length());
                context.write(okey,ovalue);
            } catch (ParseException e) {
                e.printStackTrace();
            }

        }
    }

    public static class ForReducer extends Reducer<IntWritable,AvgCount,IntWritable,AvgCount>{

             private  AvgCount ovalue=new AvgCount();

        @Override
        protected void reduce(IntWritable key, Iterable<AvgCount> values, Context context) throws IOException, InterruptedException {
            int sum=0;
            int count=0;
            for(AvgCount i:values){
                sum+=i.getCount()*i.getAvg();
                count+=i.getCount();
            }
            ovalue.setAvg(sum/count);
            ovalue.setCount(count);
            context.write(key,ovalue);

        }
    }
    public static void main(String[] args) {
        JobUtil.commitJob(AverageDriver.class,"D:\\Program Files\\feiq\\Recv Files\\MapReducer所有内容\\MapReduce基础编程 练习题及答案\\MapReduce基础编程 练习题及答案\\MapReduce基础——网站微博数据统计分析\\数据","");
    }
}
