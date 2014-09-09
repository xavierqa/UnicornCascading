package unicorn.aggregation;

import java.util.Properties;



import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.property.AppProps;

import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

public class WC {

	public static void main(String[] args) {
		
		String inputPath = args[0];
		String outputPath = args[1];
		
		Properties properties = new Properties();
		AppProps.setApplicationJarClass(properties, WC.class);
		
		
		HadoopFlowConnector flowConnector = new HadoopFlowConnector(properties);
		
		
		Tap inputTap = new Hfs( new TextDelimited(true,"\t"), inputPath);
		Tap outputTap = new Hfs(new TextDelimited(true,"\t"),outputPath);
		
		
		Fields token = new Fields("token");
		Fields text = new Fields("text");
		
		
		RegexSplitGenerator splitter = new RegexSplitGenerator(token, "[ \\[\\]\\(\\),.]");
		
		Pipe inputPipe = new Each("token", text, splitter, Fields.RESULTS);
		
		Pipe wcountPipe = new Pipe("wc", inputPipe);
		wcountPipe = new GroupBy(wcountPipe,token);
		wcountPipe = new Every(wcountPipe, Fields.ALL, new Count(), Fields.ALL);
		
		FlowDef flowDef = FlowDef.flowDef().setName("wc").addSource(inputPipe, inputTap).addTailSink(wcountPipe, outputTap);
		
		Flow wcFlowDot = flowConnector.connect(flowDef);
		wcFlowDot.writeDOT("dot/wc.dot");
		wcFlowDot.complete();
		
		
		
		
		
	}
}
