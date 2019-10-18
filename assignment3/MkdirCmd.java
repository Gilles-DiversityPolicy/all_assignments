package edu.jhu.bdpuh;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.*;

public class MkdirCmd {
    Options options = new Options();
    CommandLineParser parser = new BasicParser();

    {
        options.addOption("mkdir", false, "make directory");
        options.addOption("p",false, "create parent");
    }
    public MkdirCmd(String args[]) throws IOException {
        try {
            CommandLine line = parser.parse(options, args);

	    FileSystem fs = FileSystem.get(new Configuration());
	    if(line.hasOption("p"))
                System.out.println("has -p option");
            String[] cmdargs = line.getArgs();
            for (String a : cmdargs) {
                System.out.println(a);
		Path p = new Path ("hdfs://localhost:9000/user/" + a);
		fs.mkdirs(p);
		if (!fs.exists(p.getParent()) && line.hasOption("p")) {
		    fs.mkdirs(p.getParent());
		}
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }

    }
}
