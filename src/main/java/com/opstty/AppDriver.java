package com.opstty;

import com.opstty.job.Sort;
import com.opstty.job.WordCount;
import com.opstty.job.District;
import com.opstty.job.Species;
import com.opstty.job.NbTreePerKind;
import com.opstty.job.TallestTreePerKind;

import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");

            programDriver.addClass("district", District.class,
                    "A map/reduce program that gives the district countaining trees");

            programDriver.addClass("species", Species.class,
                    "A map/reduce program that displays the list of different species trees in this file");

            programDriver.addClass("nb_tree_per_kind", NbTreePerKind.class,
                    "A map/reduce program that displays the number of trees per kind");

            programDriver.addClass("tallest_tree_per_kind", TallestTreePerKind.class,
                    "A map/reduce program that displays the tallest tree per kind");

            programDriver.addClass("sort", Sort.class,
                    "A map/reduce program that sort Trees by height");

            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
