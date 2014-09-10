package unicorn.aggregation;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Iterator;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.CascadingTestCase;
import cascading.operation.Function;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleListCollector;

public class TestNormalization extends CascadingTestCase{

	private static Logger LOG = LoggerFactory.getLogger(TestNormalization.class);
	
	@Test
	public void testNLP(){
		Fields fieldDeclaration = new Fields( "doc_id", "token" );
	    Function scrub = new ParseData( fieldDeclaration );
	    Tuple[] arguments = new Tuple[]{
	      new Tuple( "doc_1", "is" ),
	      new Tuple( "doc_2", " are " ),
	      new Tuple( "doc_3", " O'Keefe    " ) // will be scrubed
	    };
		LOG.info("Testing");
	    /*ArrayList<Tuple> expectResults = new ArrayList<Tuple>();
	    expectResults.add( new Tuple( "doc_1", "foo" ) );
	    expectResults.add( new Tuple( "doc_1", "bar" ) );
*/
	    TupleListCollector collector = invokeFunction( scrub, arguments, Fields.ALL );
	    Iterator<Tuple> it = collector.iterator();
	    ArrayList<Tuple> results = new ArrayList<Tuple>();

	    while( it.hasNext() ){
	      results.add( it.next() );
	    }
	    LOG.info("Results {}",results.toString());
//	    assertEquals( "Scrub result is not expected", expectResults, results );
	}
}
