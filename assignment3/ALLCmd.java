package edu.jhu.bdpuh;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Date;

import java.io.*;

/**
 *
 * Usage: hadoop jar target/<jar-file> <main-class> -all <path>
 *
 * Supports the following command and arguments -all [-ignoreCrc]  [<path> ...]
 *
 *
 */
public class ALLCmd {

    private FileSystem hdfs;
    private String[] args;
    private LocalFileSystem localFileSystem;

    private LSCmd lsCmd;
    private CATCmd catCmd;
    private GETCmd getCmd;
    private RMCmd rmCmd;
    private MkdirCmd mkdirCmd;

    public ALLCmd(FileSystem hdfs, String[] args, LocalFileSystem localFileSystem) {
        this.hdfs = hdfs;
        this.args = args;
	this.localFileSystem = localFileSystem;
	
	/////////////////////
	// SAMPLE COMMANDS //
	////////////////////
	////////////////////////////////////////////////////////
	// -ls /user/data/testFile1.txt
	String[] lsStrings = new String[2];
	lsStrings[0] = "-ls";
	lsStrings[1] = String.valueOf(args[1]);
	lsCmd = new LSCmd(hdfs, lsStrings);
	////////////////////////////////////////////////////////
	// -cat /user/data/testFile1.txt
	String[] catStrings = new String[2];
	catStrings[0] = "-cat";
	catStrings[1] = String.valueOf(args[1]);	
	catCmd = new CATCmd(hdfs,catStrings, localFileSystem);
	////////////////////////////////////////////////////////
	// -get /user/data/testFile1.txt testFile.txt
	String[] getStrings = new String[3];
	getStrings[0] = "-get";
	getStrings[1] = String.valueOf(args[1]);
	getStrings[2] = "testFile.txt";
        getCmd = new GETCmd(hdfs, getStrings, localFileSystem);
	////////////////////////////////////////////////////////
	// -rm /user/data/testFile1.txt
	System.out.println("begin -rm command...");  
	String[] rmStrings = new String[2];
	rmStrings[0] = "-rm";
	rmStrings[1] = String.valueOf(args[1]);
        rmCmd = new RMCmd(hdfs, rmStrings);
	////////////////////////////////////////////////////////
    }

    public void execute() throws IOException, ParseException {
	String[] mkdirStrings = new String[2];
	mkdirStrings[0] = "-mkdir"; 
	mkdirStrings[1] = "newDirectory";
	
	System.out.println("executing -ls command...");
	this.lsCmd.execute();
	System.out.println("-ls command completed successfully");
	
	System.out.println("executing -cat command...");
	this.catCmd.execute();
	System.out.println("-cat command completed successfully");

	System.out.println("executing -mkdir command...");
	new MkdirCmd(mkdirStrings);
	System.out.println("-mkdir command completed successfully");

	System.out.println("executing -get command...");
	this.getCmd.execute();
	System.out.println("-get command completed successfully");

	System.out.println("executing -rm command...");
	this.rmCmd.execute();
	System.out.println("-rm command completed successfully");
    }

}
