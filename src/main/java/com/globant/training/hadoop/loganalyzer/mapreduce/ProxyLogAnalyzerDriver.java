package com.globant.training.hadoop.loganalyzer.mapreduce;

import com.globant.training.hadoop.loganalyzer.model.ProxyGroupedData;
import com.globant.training.hadoop.loganalyzer.model.VisitsSiteKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.StringTokenizer;

/**
 * Created by armandodelgado.
 */
public class ProxyLogAnalyzerDriver extends Configured implements Tool {

    private static final String OUTPUT_PATH_GROUP = "temp/outputgroup/sitedomaincounter";
    Logger LOGGER = LogManager.getLogger(ProxyLogAnalyzerDriver.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ProxyLogAnalyzerDriver(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {

        // Validate parameters
        if (args.length < 2) {
            System.out.println("Usage: hadoop jar globant-hadoop-proxyanalizer-1.0-SNAPSHOT-job.jar"
                    + " [generic options] <in> <out> <size scale>");
            return 1;
        }

        Configuration conf = getConf();
        // set the delimiter to "||", better for parsing
        conf.set("mapreduce.output.key.field.separator", "||");
        conf.set("mapreduce.textoutputformat.separator", "||");
        conf.set("mapreduce.output.textoutputformat.separator", "||");

        // Set the first job this job is in charge of summarizing (average) the records
        // similar to a SQL Group by
        Job jobGroup = Job.getInstance(conf, "LOGANLZR-GROUP");

        TextInputFormat.addInputPath(jobGroup, new Path(args[0]));
        TextOutputFormat.setOutputPath(jobGroup, new Path(OUTPUT_PATH_GROUP));

        jobGroup.setJarByClass(getClass());
        jobGroup.setOutputKeyClass(Text.class);
        jobGroup.setOutputValueClass(ProxyGroupedData.class);

        jobGroup.setMapOutputKeyClass(Text.class);
        jobGroup.setMapOutputValueClass(ProxyGroupedData.class);

        jobGroup.setMapperClass(ProxyLogGroupMapper.class);
        //Agregattion on the mapper before the data is sent to the Mapper,
        //this on behalf we still need to pass the #visits for sorting.
        jobGroup.setCombinerClass(ProxyLogGroupReducer.class);
        jobGroup.setReducerClass(ProxyLogGroupReducer.class);

        //jobGroup.setNumReduceTasks(10);

        if (jobGroup.waitForCompletion(true)) {

            // if the group parts works ok, then another job is setted for Ordering
            // the order is based in Total order sorting pattern and Secondary sort mechanism
            Job jobOrder = Job.getInstance(conf, "LOGANLZR-SORT");

            TextInputFormat.addInputPath(jobOrder, new Path(OUTPUT_PATH_GROUP));
            TextOutputFormat.setOutputPath(jobOrder, new Path(args[1]));

            jobOrder.setJarByClass(getClass());
            jobOrder.setOutputKeyClass(VisitsSiteKey.class);
            jobOrder.setOutputValueClass(ProxyGroupedData.class);

            jobOrder.setMapOutputKeyClass(VisitsSiteKey.class);
            jobOrder.setMapOutputValueClass(ProxyGroupedData.class);

            jobOrder.setMapperClass(ProxyLogSortMapper.class);

            // a total order partitioner is required to order the total result
            // the idea is put the top sites to the first reducer being able to use just the result from the first reducer to display the results
            // Is implemented following the "Total order sorting" pattern
            jobOrder.setPartitionerClass(TotalOrderPartitioner.class);

            InputSampler.Sampler<IntWritable, Text> sampler =
                    new InputSampler.RandomSampler<IntWritable, Text>(0.1, 10000, 10);

            InputSampler.writePartitionFile(jobOrder, sampler);

            //shuffles secondary sort mechanism
            jobOrder.setGroupingComparatorClass(VisitsCounterGroupingComparator.class);
            //jobOrder.setNumReduceTasks(3);

            if (jobOrder.waitForCompletion(true)) {
                this.printTopTen(args[1], args.length > 2 ? args[2] : "");
            }
        }

        return 1;
    }

    public int printTopTen(String filePath, String scale) {

        try {
            URI uri = URI.create(filePath);
            FileSystem fileSystem = FileSystem.get(uri, getConf());

            // we just need the result in the first reducer since we used the TotalOrderPartitioner
            // to just get the first part a filter is used
            FileStatus[] statuses = fileSystem.listStatus(new Path(uri), new PathFilter() {
                public boolean accept(Path path) {
                    return path.toString().contains("part-r-00000");
                }
            });

            // Have in mind that not all the file is loaded since we are using the Hadoop HDSF FileSystem API
            for (FileStatus status : statuses) {
                BufferedReader br = new BufferedReader(new InputStreamReader(fileSystem.open(status.getPath())));
                String line = br.readLine();

                int count = 0;
                while (line != null && count < 10) {
                    StringTokenizer stringTokenizer = new StringTokenizer(line.toString(), "||");
                    if (stringTokenizer.countTokens() == 4) {

                        StringBuffer topSiteInfo = new StringBuffer(stringTokenizer.nextElement().toString());

                        // used to show the size according to the scale passed as parameter
                        double size = Double.parseDouble(stringTokenizer.nextElement().toString());
                        String scaledSize;
                        if (scale == null || scale.equals("")) {
                            scaledSize = Double.toString(size) + "b";
                        } else if (scale.equals("K")) {
                            scaledSize = Double.toString(size / 1024) + "Kb";
                        } else if (scale.equals("M")) {
                            scaledSize = Double.toString((size / 1024) / 1024) + "Mb";
                        } else if (scale.equals("G")) {
                            scaledSize = Double.toString(((size / 1024) / 1024) / 1024) + "Gb";
                        } else {
                            scaledSize = Double.toString(size) + "b";
                        }

                        topSiteInfo.append(", " + scaledSize);
                        topSiteInfo.append(", " + stringTokenizer.nextElement().toString());
                        topSiteInfo.append(", " + stringTokenizer.nextElement().toString());

                        // Display the results
                        System.out.println(topSiteInfo);

                        line = br.readLine();
                        count++;
                    }
                }

                br.close();

                return 1;
            }
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);

            return 0;
        }

        return 1;
    }
}
