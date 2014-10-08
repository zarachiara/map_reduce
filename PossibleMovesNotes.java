/*
 * CS61C Spring 2014 Project2
 * Reminders:
 *
 * DO NOT SHARE CODE IN ANY WAY SHAPE OR FORM, NEITHER IN PUBLIC REPOS OR FOR DEBUGGING.
 *
 * This is one of the two files that you should be modifying and submitting for this project.
 */
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.lang.Math;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PossibleMoves {
    public static class Map extends Mapper<IntWritable, MovesWritable, IntWritable, IntWritable> {
        int boardWidth;
        int boardHeight;
        boolean OTurn;
        /**
         * Configuration and setup that occurs before map gets called for the first time.
         *
         **/
        @Override
        public void setup(Context context) {
            // load up the config vars specified in Proj2.java#main()
            boardWidth = context.getConfiguration().getInt("boardWidth", 0);
            boardHeight = context.getConfiguration().getInt("boardHeight", 0);
            OTurn = context.getConfiguration().getBoolean("OTurn", true);
        }

        /**
         * The map function for the first mapreduce that you should be filling out.
         */
        @Override
        public void map(IntWritable key, MovesWritable val, Context context) throws IOException, InterruptedException 
        {
            /* YOU CODE HERE */
            String board = Proj2Util.gameUnhasher(key.get(), boardWidth, boardHeight); // unhashing
            if (val.getStatus() == 0) 
            { // test if game is over 
                char[] arr = board.toCharArray(); // converting into char 
                char piece;
                if (OTurn) 
                {
                    piece = 'O';
                } else 
                {
                    piece = 'X';
                }
                ArrayList<Integer> slots = new ArrayList<Integer>();
                for (int i = 0; i < boardWidth; i++) {
                    for (int j = 0; j < boardHeight; j++) {
                        int index = i * boardHeight + j;
                        if (arr[index] == ' ') {
                            slots.add(index);
                            break;
                        }
                    }
                }
                // System.out.println("map");
                for (Integer s : slots) {
                    arr[s]=piece;
                    String newBoard = new String(arr);
                    arr[s]=' ';
                    IntWritable child = new IntWritable(Proj2Util.gameHasher(newBoard, boardWidth, boardHeight));
                    context.write(child, key);
                    // System.out.println("parent:" + Proj2Util.gameUnhasher(key.get(), boardWidth, boardHeight));
                    // System.out.println("child:" + newBoard);  
                }
                // System.out.println();
            }
        }
    }
    public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, MovesWritable> {

        int boardWidth;
        int boardHeight;
        int connectWin;
        boolean OTurn;
        boolean lastRound;
        /**
         * Configuration and setup that occurs before reduce gets called for the first time.
         *
         **/
        @Override
        public void setup(Context context) {
            // load up the config vars specified in Proj2.java#main()
            boardWidth = context.getConfiguration().getInt("boardWidth", 0);
            boardHeight = context.getConfiguration().getInt("boardHeight", 0);
            connectWin = context.getConfiguration().getInt("connectWin", 0);
            OTurn = context.getConfiguration().getBoolean("OTurn", true);
            lastRound = context.getConfiguration().getBoolean("lastRound", true);
        }

        /**
         * The reduce function for the first mapreduce that you should be filling out.
         */
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            /* YOU CODE HERE */
            ArrayList<Integer> vals = new ArrayList<Integer>();
            for (IntWritable pos : values) {
                vals.add(pos.get());
            }
            String board = Proj2Util.gameUnhasher(key.get(), boardWidth, boardHeight);
            int status;
            if (Proj2Util.gameFinished(board, boardWidth, boardHeight, connectWin)) {
                if (OTurn) {
                    status = 1;
                } else {
                    status = 2;
                }
            } else if (lastRound) {
                status = 3;
            } else {
                status = 0;
            }
            int[] parents = new int[vals.size()];
            for (int i = 0; i < vals.size(); i++) {
                parents[i] = vals.get(i);
            }
            // System.out.println("reduce");
            // System.out.println("child:" + Proj2Util.gameUnhasher(key.get(), boardWidth, boardHeight));
            // for (int i = 0; i < parents.length; i++) {
            //     System.out.println("parent:" + Proj2Util.gameUnhasher(parents[i], boardWidth, boardHeight));
            // }
            context.write(key, new MovesWritable((byte) status, parents));
        }
    }
}




