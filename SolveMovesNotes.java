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

public class SolveMoves {
    public static class Map extends Mapper<IntWritable, MovesWritable, IntWritable, ByteWritable> {
        /**
         * Configuration and setup that occurs before map gets called for the first time.
         *
         **/
        @Override
        public void setup(Context context) {
        }

        /**
         * The map function for the second mapreduce that you should be filling out.
         */
        @Override
        public void map(IntWritable key, MovesWritable val, Context context) throws IOException, InterruptedException {
            int[] parents = val.getMoves();
            for (int hash : parents) {
                context.write(new IntWritable(hash), new ByteWritable(val.getValue()));
            }

        }
    }

    public static class Reduce extends Reducer<IntWritable, ByteWritable, IntWritable, MovesWritable> {

        int boardWidth;
        int boardHeight;
        int connectWin;
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
            connectWin = context.getConfiguration().getInt("connectWin", 0);
            OTurn = context.getConfiguration().getBoolean("OTurn", true);
        }

        /**
         * The reduce function for the first mapreduce that you should be filling out.
         */
        @Override
        public void reduce(IntWritable key, Iterable<ByteWritable> values, Context context) throws IOException, InterruptedException {
            /* YOUR CODE HERE */
            ArrayList<Byte> vals = new ArrayList<Byte>(); // creates an byte array called vals 
            for (ByteWritable pos : values) { // loop for every pos in iterable values 
                vals.add(pos.get()); // add bytewritable to 
            }
            // System.out.print(key.get());
            if (!illegalParent(vals)) { // check if it is an illegal parent 
                int target;
                if (OTurn) {
                    target = 1;
                } else {
                    target = 2;
                }
                char lastPiece = ' ';
                if (OTurn) {
                    lastPiece = 'X';
                } else {
                    lastPiece = 'O';
                }
                // System.out.println(target + lastPiece + "target + lastPiece");
                // create arraylist to store wins, ties, and losses, after minimax evaluation
                ArrayList<Integer> wins = new ArrayList<Integer>();
                ArrayList<Integer> ties = new ArrayList<Integer>();
                ArrayList<Integer> losses = new ArrayList<Integer>();
                // minimax begins 
                for (Byte child : vals) { // for the child in vals 
                    int k = (int) child; // cast child into an int to k 
                    if (k % 4 == target) { // add to wins array 
                        wins.add(k);
                    } else if (k % 4 == 3) { // add to ties array
                        ties.add(k); 
                    } else if (k % 4 == 3-target) { // add to loss aray 3-1 indicates 
                        losses.add(k);
                    }
                }
                int val, status;
                if (wins.size() > 0) {
                    val = wins.get(0) >> 2;
                    for (Integer i : wins) {
                        if ((i >> 2) < val) {
                            val = i >> 2;
                            // System.out.println(val + "val wins");
                            // System.out.println(i + "i wins");
                        }
                    }
                    status = target;
                    // System.out.print(status + "wins status");
                } else if (ties.size() > 0) {
                    val = ties.get(0) >> 2;
                    for (Integer i : ties) {
                        if ((i >> 2) > val) {
                            val = i >> 2;
                            // System.out.println(val + "val ties");
                            // System.out.println(i + "i ties");
                        }
                    }
                    status = 3;
                    // System.out.print(status + "tie status");
                } else {
                    val = losses.get(0) >> 2;
                    for (Integer i : losses) {
                        if ((i >> 2) > val) {
                            val = i >> 2;
                            // System.out.println(val + "val loses");
                            // System.out.println(i + "i loses");
                        }
                    }
                    status = 3 - target;
                    // System.out.print(status + "loss status");
                }
                // 
                char[] board = Proj2Util.gameUnhasher(key.get(), boardWidth, boardHeight).toCharArray();
                ArrayList<Integer> lastMoves = new ArrayList<Integer>();
                for (int i = 0; i < boardWidth; i++) {
                    for (int j = boardHeight-1; j >=0; j--) {
                        int index = i * boardHeight + j;
                        if (board[index] != ' ') {
                            if (board[index] == lastPiece) {
                                lastMoves.add(index);
                            }
                            break;
                        }
                    }
                }        
                //
                ArrayList<Integer> ancestors = new ArrayList<Integer>();
                for (Integer a : lastMoves) {
                    board[a] = ' ';
                    String pos = new String(board);
                    board[a] = lastPiece;
                    Integer child = Proj2Util.gameHasher(pos, boardWidth, boardHeight);
                    ancestors.add(child); 
                }
                //
                int[] parents = new int[ancestors.size()];
                for (int i = 0; i < ancestors.size(); i++) {
                    parents[i] = ancestors.get(i);
                }
                //
                val += 1;
                int hash = Proj2Util.gameHasher(new String(board), boardWidth, boardHeight);
                context.write(new IntWritable(hash), new MovesWritable(status, val, parents));
            }

        }
        /**
         *  Tests for legal parents; if the "moves to end" code for the values are all nonzero,
         *  we have an illegal parent. Discard this key.
         */
        public boolean illegalParent(ArrayList<Byte> values) {
            for (Byte b : values) {
                // System.out.println(b + "bytes");
                if ((b >> 2) == 0) {
                    // System.out.println();
                    return false;
                }
            }
            // System.out.print("Illegal parent");
            return true;
        }
    }
}
