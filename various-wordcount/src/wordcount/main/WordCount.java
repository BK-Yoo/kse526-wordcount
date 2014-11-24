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

//        public void setFrequency(int frequency){ this.frequency = frequency; }
//
//        public void setText(String text){ this.text = text; }

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
            // if text of word is same, return 0(consider that they are same).
            if(this.equals(o)) {
                return 0;

            // texts of both Word instances are different but the frequency is same, return 1 for allowing duplicate word.
            } else if (compareFrequency(o, this) == 0){
                return 1;

            } else {
                // descending order
                return compareFrequency(o, this);

            }
        }

        private int compareFrequency(Word a, Word b){
            return a.getFrequency() - b.getFrequency();
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
            line = value.toString();
            result = line.split(" ");

            for(String targetWord : result) {
                targetWord = targetWord.trim();
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

        //There is some bugs, that combiner and reducer share the same words instance.
        //So make words instance private for each worker.
        private SortedSet<Word> words = new TreeSet<Word>();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // if the sum frequency of key is above the frequency of the 100th word,
            // write that key-value pair to reduce function.
            sum = 0;
            for(IntWritable value : values)
                sum += value.get();

            addWordToSortedSet(new Word(key.toString(), sum));

            if (sum >= words.last().getFrequency()) {
                tempValue.set(sum);
                context.write(key, tempValue);
            }
        }

        private void addWordToSortedSet(Word newWord){
            words.add(newWord);

            if(words.size() > TOP_100)
                // last element has the smallest frequency among the words.
                words.remove(words.last());
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        // Variables for reducing the cost of creating instance.
        protected Text tempKey = new Text();
        protected IntWritable tempValue = new IntWritable();
        protected int sum = 0;

        // Constant for checking the size of words.
        protected final int TOP_100 = 100;

        // Container of word for 100 most frequent words.
        private SortedSet<Word> words = new TreeSet<Word>();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            sum = 0;
            for(IntWritable value : values)
                sum += value.get();

            addWordToSortedSet(new Word(key.toString(), sum));
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            emitWords(context);
        }

        protected void emitWords(Context context) throws IOException, InterruptedException {
            for(Word word : words) {
                tempKey.set(word.getText());
                tempValue.set(word.getFrequency());
                context.write(tempKey, tempValue);
            }
        }

        private void addWordToSortedSet(Word newWord){
            words.add(newWord);

            if(words.size() > TOP_100)
                // last element has the smallest frequency among the words.
                words.remove(words.last());
        }

    }

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

            //Check whether job will use combiner functions or not.
            if(args[index].equals("-combiner"))
                //if the "-combiner" option exists, set using combiner class flag to true.
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
        job.setJarByClass(WordCount.class);

        job.setJobName("wordcount");

        job.setJarByClass(WordCount.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //Set 1 for number of reduce task for keeping 100 most words in sorted set.
        job.setNumReduceTasks(1);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        if(useCombinerClass)
            job.setCombinerClass(Combiner.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);
    }
}
