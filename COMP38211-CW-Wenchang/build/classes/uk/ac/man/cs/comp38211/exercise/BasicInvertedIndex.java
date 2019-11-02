/**
 * Basic Inverted Index
 * 
 * This Map Reduce program should build an Inverted Index from a set of files.
 * Each token (the key) in a given file should reference the file it was found 
 * in. 
 * 
 * The output of the program should look like this:
 * sometoken [file001, file002, ... ]
 * 
 * @author Kristian Epps
 */
package uk.ac.man.cs.comp38211.exercise;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import uk.ac.man.cs.comp38211.io.array.ArrayListOfLongsWritable;
import uk.ac.man.cs.comp38211.io.array.ArrayListWritable;
import uk.ac.man.cs.comp38211.io.map.HashMapWritable;
import uk.ac.man.cs.comp38211.io.pair.PairOfStrings;
import uk.ac.man.cs.comp38211.io.pair.PairOfWritables;
import uk.ac.man.cs.comp38211.ir.Stemmer;
import uk.ac.man.cs.comp38211.ir.StopAnalyser;
import uk.ac.man.cs.comp38211.util.XParser;

public class BasicInvertedIndex extends Configured implements Tool
{
    private static final Logger LOG = Logger
            .getLogger(BasicInvertedIndex.class);

    public static class Map extends 
            Mapper<Object, Text, Text, HashMapWritable<Text, ArrayListWritable<IntWritable>>>
    {
        // INPUTFILE holds the name of the current file
        private final static Text INPUTFILE = new Text();
        
        // TOKEN should be set to the current token rather than creating a 
        // new Text object for each one
        @SuppressWarnings("unused")
        private final static Text TOKEN = new Text();

        // The StopAnalyser class helps remove stop words
        @SuppressWarnings("unused")
        private StopAnalyser stopAnalyser = new StopAnalyser();
        
        // The stem method wraps the functionality of the Stemmer
        // class, which trims extra characters from English words
        // Please refer to the Stemmer class for more comments
        @SuppressWarnings("unused")
        private String stem(String word)
        {
            Stemmer s = new Stemmer();

            // A char[] word is added to the stemmer with its length,
            // then stemmed
            s.add(word.toCharArray(), word.length());
            s.stem();

            // return the stemmed char[] word as a string
            return s.toString();
        }
        
        // This method gets the name of the file the current Mapper is working
        // on
        @Override
        public void setup(Context context)
        {
            String inputFilePath = ((FileSplit) context.getInputSplit()).getPath().toString();
            String[] pathComponents = inputFilePath.split("/");
            INPUTFILE.set(pathComponents[pathComponents.length - 1]);
        }
        
        // TODO
        // This Mapper should read in a line, convert it to a set of tokens
        // and output each token with the name of the file it was found in
        private HashMap<Text, ArrayListWritable<IntWritable>> localCache = new HashMap<Text, ArrayListWritable<IntWritable>>();
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException
        {
        	String line = value.toString();
        	StringTokenizer itr = new StringTokenizer(line);
        	
        	Integer pos_index = 0;
        	// Integer cnt1 = 0, cnt2 = 0;
        	while(itr.hasMoreTokens()) {
        		String currentToken = itr.nextToken();
        		pos_index++;
        		
        		// replace all non-alphabetic symbols with ""
            	currentToken = currentToken.replaceAll("[^a-zA-Z]", "");
            	
            	// case folding
            	// referenced from stanford nlp:
            	// However, trying to get capitalization right in this way probably doesn't help
            	// if your users usually use lowercase regardless of the correct case of words. 
            	// Thus, lowercasing everything often remains the most practical solution.
            	// convert all letters to lower case
            	currentToken = currentToken.toLowerCase();
            	
//            	// stemming
//            	currentToken = stem(currentToken);
//            	// remove stopwords
//            	if (StopAnalyser.isStopWord(currentToken)) {
//            		continue;
//            	}
            	
        		Text keyToken = new Text(currentToken);
        		
        		// in-mapped aggregation
        		if (localCache.containsKey(keyToken)) {
        			// cnt1++;
        			localCache.get(keyToken).add(new IntWritable(pos_index));
        		} else {
        			// cnt2++;
        			ArrayListWritable<IntWritable> newArrayListW = new ArrayListWritable<IntWritable>();
        			newArrayListW.add(new IntWritable(pos_index));
        			localCache.put(keyToken, newArrayListW);
        		}
        		
        		// assume key is the filename, WORD is each token
        		// context.write(TOKEN, INPUTFILE);
        	}
        	// System.out.println("all:" + pos_index.toString());
        	// System.out.println("in mapper:" + cnt1.toString());
        	// System.out.println("created new pair:" + cnt2.toString());
        	
        	// in-mapped aggregation cont.
        	// iterate hashmap and write map results
        	// Integer cnt0 = 0;
        	Iterator<java.util.Map.Entry<Text, ArrayListWritable<IntWritable>>> iterator = localCache.entrySet().iterator();
        	while(iterator.hasNext()) {
        		// cnt0++;
        		java.util.Map.Entry<Text, ArrayListWritable<IntWritable>> entry = iterator.next();
        		TOKEN.set(entry.getKey());
        		ArrayListWritable<IntWritable> valuePos = new ArrayListWritable<IntWritable>(entry.getValue());
        		HashMapWritable<Text, ArrayListWritable<IntWritable>> filePosMap = new HashMapWritable<Text, ArrayListWritable<IntWritable>>();
        		filePosMap.put(INPUTFILE, valuePos);
        		// System.out.println(keyToken);
        		context.write(TOKEN, filePosMap);
        	}
        	// System.out.println("number of entries in map: " + cnt0.toString());
        }
    }

    public static class Reduce extends Reducer<Text, HashMapWritable<Text, ArrayListWritable<IntWritable>>, Text, ArrayListWritable<HashMapWritable<Text, ArrayListWritable<IntWritable>>>>
    {
    	private final static ArrayListWritable<HashMapWritable<Text, ArrayListWritable<IntWritable>>> iIndex = new ArrayListWritable<HashMapWritable<Text, ArrayListWritable<IntWritable>>>();
    	
    	// TODO
        // This Reduce Job should take in a key and an iterable of file names
        // It should convert this iterable to a writable array list and output
        // it along with the key
        public void reduce(
                Text key,
                Iterable<HashMapWritable<Text, ArrayListWritable<IntWritable>>> values,
                Context context) throws IOException, InterruptedException
        {
        	Iterator<HashMapWritable<Text, ArrayListWritable<IntWritable>>> itr = values.iterator();
        	HashMapWritable<Text, ArrayListWritable<IntWritable>> summarizeMap = new HashMapWritable<Text, ArrayListWritable<IntWritable>>(itr.next());
        	while(itr.hasNext()) {
        		// get each pair of <filename, posIndex> 
        		Iterator<java.util.Map.Entry<Text, ArrayListWritable<IntWritable>>> filenameIterator = itr.next().entrySet().iterator();
                while(filenameIterator.hasNext()) {
                	java.util.Map.Entry<Text, ArrayListWritable<IntWritable>> entry = filenameIterator.next();
            		Text fileName = new Text(entry.getKey());
            		ArrayListWritable<IntWritable> valuePos = new ArrayListWritable<IntWritable>(entry.getValue());
            		if(summarizeMap.containsKey(fileName)) {
            			// if already have this fileName in the hashmap
            			summarizeMap.get(fileName).addAll(valuePos);
            		} else {
            			// else create a new pair in hashmap
            			summarizeMap.put(fileName, valuePos);
            		}
                }
        	}
        	// System.out.println("reduced token: " + key);
        	iIndex.add(summarizeMap);
        	context.write(key, iIndex);
        	iIndex.removeAll(iIndex);
        }
    }

    // Lets create an object! :)
    public BasicInvertedIndex()
    {
    }

    // Variables to hold cmd line args
    private static final String INPUT = "input";
    private static final String OUTPUT = "output";
    private static final String NUM_REDUCERS = "numReducers";

    @SuppressWarnings({ "static-access" })
    public int run(String[] args) throws Exception
    {
        
        // Handle command line args
        Options options = new Options();
        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("input path").create(INPUT));
        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("output path").create(OUTPUT));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("number of reducers").create(NUM_REDUCERS));

        CommandLine cmdline = null;
        CommandLineParser parser = new XParser(true);

        try
        {
            cmdline = parser.parse(options, args);
        }
        catch (ParseException exp)
        {
            System.err.println("Error parsing command line: "
                    + exp.getMessage());
            System.err.println(cmdline);
            return -1;
        }

        // If we are missing the input or output flag, let the user know
        if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT))
        {
            System.out.println("args: " + Arrays.toString(args));
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        // Create a new Map Reduce Job
        Configuration conf = new Configuration();
        Job job = new Job(conf);
        String inputPath = cmdline.getOptionValue(INPUT);
        String outputPath = cmdline.getOptionValue(OUTPUT);
        int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ? Integer
                .parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 1;

        // Set the name of the Job and the class it is in
        job.setJobName("Basic Inverted Index");
        job.setJarByClass(BasicInvertedIndex.class);
        job.setNumReduceTasks(reduceTasks);
        
        // Set the Mapper and Reducer class (no need for combiner here)
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        
        // Set the Output Classes
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(HashMapWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ArrayListWritable.class);

        // Set the input and output file paths
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        
        // Time the job whilst it is running
        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime)
                / 1000.0 + " seconds");

        // Returning 0 lets everyone know the job was successful
        return 0;
    }

    public static void main(String[] args) throws Exception
    {
        ToolRunner.run(new BasicInvertedIndex(), args);
    }
}
