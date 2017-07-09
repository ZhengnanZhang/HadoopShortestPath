package comp9313.ass2;



import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


//import ...

public class SingleTargetSP {


    public static String OUT = "output";
    public static String IN = "input";
    public static String QUERY = "query";
    static enum eInf{
    	COUNTER
    }

    public static class STMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
           int conuter = context.getConfiguration().getInt("run.counter",1);
           QUERY = context.getConfiguration().get("targetNode");
           int locationnum = 0;
           int locationnum1 = 0;
           String str = null;
           String substr = null;
           String line = value.toString();
           String[] parts = StringUtils.split(line);
           String distance = "Infinity";
        //judge if this is the first time, if it is , we initial the input
        if (conuter == 1){
           if(parts[2].toString().equals(QUERY)){
            //set the target node's distance to 0, others Infinity
        	   distance = "0.0";
           }else {
        	distance = "Infinity";
           }
           str = distance + "\t" + "("+parts[1]+":"+parts[3]+")";
           locationnum = Integer.parseInt(parts[2].toString());
           
           
           context.write(new Text(Integer.toString(locationnum)),new Text(str));
       }else{
           substr = value.toString();
           locationnum = Integer.parseInt(parts[0].toString());
           distance = parts[1].toString();
           String[] strs11 = StringUtils.split(substr);
           str = " ";
           for (int q = 1; q < strs11.length;q++){
            //get the adjacency list and then emit
        	   str = str + strs11[q]+"\t";
           }
           context.write(new Text(Integer.toString(locationnum)),new Text(str));
       }
       if(distance.equals("Infinity"))
        return;
       //emit every pair of the adjacency list, like (4:2.0) in order for reducer to generate the minimum distance
       String[] strs = StringUtils.split(str);
       for (int i = 1; i < strs.length;i++){
    	    String k1 = strs[i].toString();
            String k = k1.substring(k1.indexOf("(")+1,k1.indexOf(":"));
            
            String v = new String(Double.parseDouble(k1.substring(k1.indexOf(":")+1,k1.indexOf(")")))+Double.parseDouble(distance)+" ");
            locationnum1 = Integer.parseInt(k);
            context.write(new Text(Integer.toString(locationnum1)),new Text(v));
       }

    }
    }


    public static class STReducer extends Reducer<Text, Text, Text, Text> {
        
        @Override

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String min = null;
            int i = 0;
            String dis = "Infinity";
            String abc = " ";
            String[] adjacency = null;
//            String[] compare = null;
            String distance1 = null;
            for (Text val: values){
//            	System.out.println(val);
                i++;
                dis = StringUtils.split(val.toString())[0];
                
                
                String[] strs = StringUtils.split(val.toString());
                if (strs.length > 1){
                    adjacency = strs;
                }
                if (strs.length>1){
                  //get the adjacency list
                for (int a = 1; a<adjacency.length;a++ ){
                    abc = abc + adjacency[a]+"\t";
                    
                }
                }
                //get the value of the min
                if (i == 1){
                    min = dis;
                } else{
                    if (dis.equals("Infinity"))
                        ;
                    else if (min.equals("Infinity"))
                        min = dis;
                    else if (Double.parseDouble(min) > Double.parseDouble(dis)){
                        min = dis;
                    }
                    }
            }
            //add the value of the counter to express there has change
            if (adjacency!=null){
            if (!min.equals("Infinity")){
//            	System.out.println(adjacency[0]);
                if (adjacency[0].equals("Infinity")){
                	
                    context.getCounter(eInf.COUNTER).increment(1L);
                }else{
                	
                    if (Double.parseDouble(adjacency[0])>Double.parseDouble(min)){
                    	
                    	
                        context.getCounter(eInf.COUNTER).increment(1L);
                    }
                }
            }
            }
            distance1 = " "+min+"\t"+abc;
            context.write(key, new Text(distance1));
        }
    }
   public static class FinalMapper extends Mapper<Object, Text, LongWritable, Text>{
	   public void map (Object key, Text value, Context context) throws IOException, InterruptedException{
		   String line = value.toString();
		   String[] sp = StringUtils.split(line);
		   
		   //only emit the node id and the distance
		   long firstnum = Long.parseLong(sp[0]);
		   context.write(new LongWritable(firstnum), new Text(sp[1]));
				   
	   }
   }
   
   public static class FinalReducer extends Reducer<LongWritable, Text, Text, Text>{
	   Text finalstr = new Text();
	   public void reduce (LongWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException{
		   //get the target node id, and emit as the key
       QUERY = context.getConfiguration().get("targetNode");
       String fir = QUERY;
		   
		   for (Text val:value){
			  
			   if (!val.toString().equals("Infinity")){
				   
			   finalstr.set(key.toString()+"\t"+val);
			   context.write(new Text(fir), new Text(finalstr));
		   }
				   
	   }
   }  
   }
            
       


    public static void main(String[] args) throws Exception {        

        IN = args[0];

        OUT = args[1];
        
        QUERY = args[2];
        

        String input = IN;

        String output = OUT + System.nanoTime();
        
        int b = 0;
        long num = 1;
        long tmp = 0;
        boolean isdone = false;
        while(num>0){
          //set the QUERY in order to let mapper knows how to initial the input
            b++;
            Configuration conf = new Configuration();
            conf.set("targetNode", QUERY);
          //justify if this is the first time of compute
            if (b == 1){
            conf.setInt("run.counter", 1);
            }else{
              //if this is not the first time, change the value makes the mapper know 
            	conf.setInt("run.counter", 3);
            }
            Job job = Job.getInstance(conf,"SingleTargetSP");
            job.setJarByClass(SingleTargetSP.class);
            job.setMapperClass(STMapper.class);
            job.setReducerClass(STReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass( Text.class);
            
            FileInputFormat.addInputPath(job, new Path(input));
            
            FileOutputFormat.setOutputPath(job, new Path(output));
            // makes the last output become the next time input
            input = output;           
            // generate several outputfile with differnt nanoTime
            output = OUT + System.nanoTime();
        
	    // YOUR JOB: Convert the input file to the desired format for iteration, i.e., 
        //           create the adjacency list and initialize the distances
        // ... ...
            //judge if one of the map reduce have completed
            boolean c = job.waitForCompletion(true);
            if (c){
              //get the value of the counter
            	num = job.getCounters().findCounter(eInf.COUNTER).getValue();
            	
            	if (num == 0){
            		isdone = true;
            		
            	}
            }

            

            // YOUR JOB: Configure and run the MapReduce job
            // ... ...                   
            


            //You can consider to delete the output folder in the previous iteration to save disk space.

            // YOUR JOB: Check the termination criterion by utilizing the counter
            // ... ...

//          if(the termination condition is reached){
//              isdone = true;
//        }
     

        // YOUR JOB: Extract the final result using another MapReduce job with only 1 reducer, and store the results in HDFS
        // ... ...
            
        }
        //if there is no change in the iteration, we move to the fianl step
        if (isdone = true){
        	Configuration conf_final = new Configuration();
        	conf_final.set("targetNode",QUERY);
        	
        	Job job_final = Job.getInstance(conf_final,"SingleTargetSP");
        	job_final.setJarByClass(SingleTargetSP.class);
        	job_final.setMapperClass(FinalMapper.class);
        	job_final.setReducerClass(FinalReducer.class);
        	job_final.setMapOutputKeyClass(LongWritable.class);
        	job_final.setMapOutputValueClass(Text.class);
        	//set the number of the Reducers to 1;
        	job_final.setNumReduceTasks(1);
        	FileInputFormat.addInputPath(job_final, new Path(input));
        	FileOutputFormat.setOutputPath(job_final, new Path(OUT));
        	System.exit(job_final.waitForCompletion(true)?0:1);
        }
        
        
  }
    
}

