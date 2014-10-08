package unicorn.aggregation;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import weka.core.Stopwords;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.CoreAnnotations.LemmaAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

public class ParseData extends BaseOperation implements Function {

	public static Logger LOG = LoggerFactory.getLogger(ParseData.class);

	public ParseData(Fields fieldDeclaration) {
		super(2, fieldDeclaration);
	}

	@Override
	public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
		// TODO Auto-generated method stub
		TupleEntry argument = functionCall.getArguments();
		String url = argument.getString(0);
		String token = argument.getString(1);

		if (token.length() > 0) {
			token = lowerCase(token);
			Tuple result = new Tuple();
			result.add(url);
			result.add(token);

			functionCall.getOutputCollector().add(result);
//				String nlp = NLP(token);
//				if (nlp != null) {
//					LOG.info("NO STOP WORD {} ", nlp);
//					//
//					functionCall.getOutputCollector().add(result);
//				} else {
//					return;
//				}
			
		}

	}

	public String lowerCase(String token) {
		return token.trim().toLowerCase();
	}

	public String NLP(String text) {

		Stopwords stop = new Stopwords();
		StanfordCoreNLP pipeline;
		Properties props;
		props = new Properties();
		// props.setProperty("annotators",
		// "tokenize, ssplit, parse, sentiment");
		props.setProperty("annotators", "tokenize, ssplit,pos,lemma");

		// StanfordCoreNLP loads a lot of models, so you probably
		// only want to do this once per execution
		pipeline = new StanfordCoreNLP(props);

		// List<String> lemmas = new LinkedList<String>();
		String documentText = text;
		// create an empty Annotation just with the given text
		Annotation document = new Annotation(documentText);

		// run all Annotators on this text
		pipeline.annotate(document);

		// Iterate over all of the sentences found
		List<CoreMap> sentences = document.get(SentencesAnnotation.class);
		String lemma = null;
		LOG.info("NLP Testing {}", documentText);

		for (CoreMap sentence : sentences) {

			// Iterate over all tokens in a sentence
			for (CoreLabel token : sentence.get(TokensAnnotation.class)) {

				// Retrieve and add the lemma for each word into the list of
				// lemmas
				lemma = token.get(LemmaAnnotation.class).toLowerCase();
				LOG.info("DATA {}", lemma);
				if (StringUtils.isAlpha(lemma)) {
					if (!stop.isStopword(lemma)) {
						 LOG.info("DATA OUTPUT {}",lemma);
						return lemma;

					}else{
						lemma = null;
					}
				}else{
					lemma = null;
				}
			}
		}
		return lemma;

	}

}
