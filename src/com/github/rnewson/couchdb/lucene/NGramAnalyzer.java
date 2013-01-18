package com.github.rnewson.couchdb.lucene;

import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ngram.NGramTokenFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.util.Version;

public class NGramAnalyzer extends Analyzer {

	private Version version;

	// private static final Set<?> STOP_WORDS;
	// static {
	// CharArraySet stopSet = new CharArraySet(0, false);
	// STOP_WORDS = CharArraySet.unmodifiableSet(stopSet);
	// }

	public NGramAnalyzer(Version version) {
		this.version = version;
	}

	@Override
	public TokenStream tokenStream(String fieldName, Reader reader) {
		TokenStream result = new StandardTokenizer(this.version, reader);
		result = new NGramTokenFilter(result, 2, 2);
		result = new LowerCaseFilter(result);
		//result = new StopFilter(this.version, result, STOP_WORDS);
		return result;
	}
}
