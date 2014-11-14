package wordcount.main;

/**
 * Created by bk on 14. 11. 13.
 * Write Code for kse526(Bigdata analysis group assingment)
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.SortedSet;
import java.util.TreeSet;

public class WordCount {

    public static class Word implements Comparable<Word>{
        private final int frequency;
        private final String text;

        public Word(String text, int frequency){
            this.text = text;
            this.frequency = frequency;
        }

        public int getFrequency(){
            return this.frequency;
        }

        public String getText(){
            return this.text;
        }

        @Override
        public int hashCode() {
            return text.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if(this == obj)
                return true;
            if(obj instanceof Word)
                return this.getText().equals(((Word) obj).getText());

            return false;
        }

        @Override
        public int compareTo(Word o) {
            // if text of word is same, return 0(think they are same).
            if(this.equals(o))
                return 0;

            int compareResult = Integer.compare(o.getFrequency(), this.getFrequency());

            // only the frequency is same, then return 1 for allowing duplicate word.
            if(compareResult == 0)
                return 1;

            return compareResult;
        }
    }

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        public final static String WORDLENGTH = "wordlength";
        public final static String WORDPREFIX = "wordprefix";

        private final static IntWritable one = new IntWritable(1);

        private Text word = new Text();
        private String[] result;
        private String line;

        private int lengthOfWord = -1;
        private String prefixOfWord = "";

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            line = value.toString().trim();
            result = line.split("\\s");

            for(String targetWord : result) {
                if(targetWord.equals(""))
                    continue;

                if(isMatchedToGivenConditions(targetWord)){
                    word.set(targetWord);
                    context.write(word, one);
                }
            }
        }

        @Override
        protected void setup(Context context){
            lengthOfWord = context.getConfiguration().getInt(WORDLENGTH, -1);
            prefixOfWord = context.getConfiguration().get(WORDPREFIX, "");
        }

        private boolean isMatchedToGivenConditions(String targetWord){
            return (satisfyLengthCondtion(targetWord) &&
                    satisfyPrefixCondition(targetWord));
        }

        private boolean satisfyLengthCondtion(String targetWord){
            return ((lengthOfWord == -1) || (targetWord.length() == lengthOfWord));
        }

        private boolean satisfyPrefixCondition(String targetWord){
            return ((prefixOfWord.equals("")) || (targetWord.startsWith(prefixOfWord)));
        }
    }

    public static class Combiner extends Reduce{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            super.reduce(key, values, context);
            // if the sum of key above the 100th frequency,
            // then we write that key-value pair to reduce function.
            if(sum >= words.last().getFrequency())
                context.write(key, new IntWritable(sum));
            }

    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final int TOP_100 = 100;
        SortedSet<Word> words = new TreeSet<Word>();
        protected int sum = 0;

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            sum = 0;
            for(IntWritable value : values)
                sum += value.get();

            words.add(new Word(key.toString(), sum));

            if(words.size() > TOP_100)
                words.remove(words.last());
        }

        @Override
        public void cleanup(Reducer.Context context) throws IOException, InterruptedException {
            emitWords(context);
        }

        protected void emitWords(Context context) throws IOException, InterruptedException {
            for(Word word : words)
                context.write(new Text(word.getText()), new IntWritable(word.getFrequency()));
        }

    }


    //commit main
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        boolean useCombinerClass = false;
        Path inputPath = null;
        Path outputPath = null;
        for(int index = 0; index < args.length; index++){

            //Extract input path string from command line.
            if(args[index].equals("-input"))
                inputPath = new Path(args[index + 1]);

            //Extract output path string from command line.
            if(args[index].equals("-output"))
                outputPath = new Path(args[index + 1]);

            //Check whether job uses combiner functions.
            if(args[index].equals("-combiner"))
                //if the "-combiner" option exists, set combiner class flag to true.
                useCombinerClass = true;

            //Extract the length of target words.
            if(args[index].equals("-word-length")) {
                conf.set(Map.WORDLENGTH, args[index + 1]);
            }
            //Extract the prefix of target words.
            if(args[index].equals("-prefix"))
                conf.set(Map.WORDPREFIX, args[index + 1]);
        }

        Job job = new Job(conf);

        job.setJobName("wordcount");

        job.setJarByClass(WordCount.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        if(useCombinerClass)
            job.setCombinerClass(Combiner.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true)? 0 : 1);
    }
}
