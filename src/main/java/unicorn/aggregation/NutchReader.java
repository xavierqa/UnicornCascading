package unicorn.aggregation;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.CrawlDb;
import org.apache.nutch.parse.ParseText;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.DebugLevel;
import cascading.operation.Insert;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.CountBy;
import cascading.pipe.assembly.Rename;
import cascading.pipe.assembly.Retain;
import cascading.pipe.assembly.SumBy;
import cascading.pipe.assembly.Unique;
import cascading.property.AppProps;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.scheme.hadoop.SequenceFile;
import cascading.scheme.hadoop.TextDelimited;
import cascading.scheme.hadoop.WritableSequenceFile;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;

public class NutchReader {

	public static void main(String[] args) {

		String inputPath = args[0];
		String outputPath = args[1];
	//	String tfoutput = args[2];
		
		
		Properties properties = new Properties();
		AppProps.setApplicationJarClass(properties, NutchReader.class);
		FlowConnector flowConnector = new HadoopFlowConnector(properties);

	//	FlowConnector flowConnector = new HadoopFlowConnector();
		
		// read data from nutch and create a field "rawdata"
		Fields nutchfield = new Fields("url", "parse-text");
		WritableSequenceFile schema = new WritableSequenceFile(nutchfield, ParseText.class, Text.class);
		Tap nutchTap = new Lfs(schema, inputPath);

		// define output tap
		Tap wcTap = new Lfs(new TextDelimited(true,"\t"), outputPath);
		
		// TF path 
		//Tap tfPath = new Lfs (new TextDelimited(true,"\t"),tfoutput);
		
		// Operation to split the data
		Fields token = new Fields("token");
		Fields text = new Fields("text");
		Fields output = new Fields("url", "token");
		RegexSplitGenerator splitter = new RegexSplitGenerator(token,"[ \\[\\]\\(\\).,]");

		Pipe docPipe = new Each("token", new Fields("parse-text"), splitter, output);

		docPipe = new Each(docPipe, output, new ParseData(output), Fields.RESULTS);
		
		docPipe = new Retain(docPipe, output);
		// word count 
		
		//Term Frequency
		Pipe tfPipe = new Pipe("TF",docPipe);
		Fields tf_count = new Fields( "tf_count" );
		tfPipe = new CountBy(tfPipe,output,tf_count);
		Fields tf_token = new Fields("tf_token");
		tfPipe = new Rename( tfPipe, token, tf_token );

		
		//Count documents
		Fields url = new Fields("url");
		//current counter
		Fields tally = new Fields("tally");
		Fields rhs_join = new Fields( "rhs_join" );
	    Fields n_docs = new Fields( "n_docs" );
	    
	    Pipe dPipe = new Unique("Dcount",docPipe,url);
	    dPipe = new Each(dPipe, new Insert(tally, 1),Fields.ALL);
	    dPipe = new Each(dPipe, new Insert(rhs_join,1), Fields.ALL);
	    dPipe = new SumBy(dPipe, tally,rhs_join,n_docs,long.class);
		//Pipe wcPipe = new Pipe("copy", docPipe);
		
/*		Pipe wcPipe = new Pipe("wc",docPipe);
		wcPipe = new GroupBy(wcPipe, token);
		wcPipe = new Every(wcPipe, Fields.ALL, new Count(), Fields.ALL);*/
		
		
		

		FlowDef flowDef = FlowDef.flowDef()
				.setName("wc")
				.addSource(docPipe, nutchTap)
				.addTailSink(dPipe, wcTap);
		
		flowDef.setDebugLevel(DebugLevel.VERBOSE);
		//test
		Flow wcFlowDot = flowConnector.connect(flowDef);
		wcFlowDot.writeDOT("dot/wc.dot");
		wcFlowDot.complete();

	}
}
