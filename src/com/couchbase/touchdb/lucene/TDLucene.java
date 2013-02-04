package com.couchbase.touchdb.lucene;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.json.JSONException;
import org.json.JSONObject;

import android.app.Activity;
import android.content.Context;
import android.content.SharedPreferences;
import android.os.AsyncTask;
import android.util.Log;

import com.couchbase.touchdb.TDServer;
import com.couchbase.touchdb.ektorp.TouchDBHttpClient;
import com.couchbase.touchdb.router.TDURLStreamHandlerFactory;
import com.github.rnewson.couchdb.lucene.Config;
import com.github.rnewson.couchdb.lucene.DatabaseIndexer;
import com.github.rnewson.couchdb.lucene.PathParts;
import com.github.rnewson.couchdb.lucene.couchdb.Couch;
import com.github.rnewson.couchdb.lucene.couchdb.Database;
import com.github.rnewson.couchdb.lucene.util.ServletUtils;

public class TDLucene {

	private static String LOG_TAG = "TDLUCENE";

	static {
		TDURLStreamHandlerFactory.registerSelfIgnoreError();
	}

	private final Map<Database, DatabaseIndexer> indexers = new HashMap<Database, DatabaseIndexer>();
	private final Map<Database, Thread> threads = new HashMap<Database, Thread>();
	private TouchDBHttpClient client;

	public TDLucene(TDServer server) throws MalformedURLException {
		this.client = Config.getClient(server);
	}

	private synchronized DatabaseIndexer getIndexer(final Database database)
			throws IOException, JSONException {
		DatabaseIndexer result = indexers.get(database);
		Thread thread = threads.get(database);
		if (result == null || thread == null || !thread.isAlive()) {
			result = new DatabaseIndexer(client, Config.getDir(), database);
			thread = new Thread(result);
			thread.start();
			result.awaitInitialization();
			if (result.isClosed()) {
				return null;
			} else {
				indexers.put(database, result);
				threads.put(database, thread);
			}
		}

		return result;
	}

	private Couch getCouch(TDLuceneRequest req) throws IOException {
		final String sectionName = new PathParts(req).getKey();
		// final Configuration section = ini.getSection(sectionName);
		// if (!section.containsKey("url")) {
		// throw new FileNotFoundException(sectionName
		// + " is missing or has no url parameter.");
		// }
		return new Couch(client, Config.url);
	}

	private DatabaseIndexer getIndexer(TDLuceneRequest req) throws IOException,
			JSONException {
		final Couch couch = getCouch(req);
		final Database database = couch.getDatabase("/"
				+ new PathParts(req).getDatabaseName());
		return getIndexer(database);
	}

	public void process(Context mContext, TDLuceneRequest req, Callback callback)
			throws IOException, JSONException {
		TDLuceneAsync async = new TDLuceneAsync(mContext, callback);
		async.execute(req);
	}

	private class TDLuceneAsync extends
			AsyncTask<TDLuceneRequest, Integer, Object> {

		private Callback callback;
		private String error;
		private Context mContext;

		public TDLuceneAsync(Context mContext, Callback callback) {
			this.callback = callback;
			this.mContext = mContext;
		}

		@Override
		protected Object doInBackground(TDLuceneRequest... params) {
			
		
			TDLuceneRequest req = params[0];
			
			Log.d(LOG_TAG,"Received a request: " + req.getUrl());
			
			try {
				// TODO Make this work for multiple dbs
				ObjectNode resp = JsonNodeFactory.instance.objectNode();

				if (indexingNow(mContext)) {
					Log.d(LOG_TAG,"Indexing at the moment");
					
					// If we are indexing, we can get the status of the index
					if ("info".equals(req.getFunction())) {
						resp.put("update_seq", indexingState(mContext));
						return new JSONObject(resp.toString());
					} else {
						error = "Sorry, I am still indexing";
						return null;
					}
				} else {
					
					Log.d(LOG_TAG,"Not indexing at the moment");

					DatabaseIndexer indexer = getIndexer(req);

					if (indexer == null) {
						ServletUtils.sendJsonError(req, resp, 500,
								"error_creating_index");
						return new JSONObject(resp.toString());
					}

					if (!"ok".equals(req.getParamAsString("stale"))) {
						indexer.updateIndexes(this.mContext);
					}

					if ("query".equals(req.getFunction())) {
						String json = indexer.search(req);
						return new JSONObject(json);
					} else if ("info".equals(req.getFunction())) {
						indexer.info(req, resp);
						return new JSONObject(resp.toString());
					} else if ("optimize".equals(req.getFunction())
							|| "expunge".equals(req.getFunction())) {
						indexer.admin(req, resp);
						return new JSONObject(resp.toString());
					}
				}

			} catch (IOException e) {
				error = e.getLocalizedMessage();
				e.printStackTrace();
			} catch (JSONException e) {
				error = e.getLocalizedMessage();
				e.printStackTrace();
			}

			return null;
		}

		@Override
		protected void onPostExecute(Object result) {

			if (callback != null) {
				if (result != null) {
					callback.onSucess(result);
				} else {
					callback.onError(error);
				}
			}
		}
	}

	public static boolean indexingNow(android.content.Context mContext) {
		final SharedPreferences prefs = mContext.getSharedPreferences("lucene",
				Activity.MODE_WORLD_READABLE);
		return prefs.getBoolean("indexing", false)
				&& (System.currentTimeMillis() - prefs.getLong("indexing_time",
						0)) < 2 * 60 * 1000;
	}

	public static String indexingState(android.content.Context mContext) {
		final SharedPreferences prefs = mContext.getSharedPreferences("lucene",
				Activity.MODE_WORLD_READABLE);
		return prefs.getString("index_at", "0");
	}

	public static abstract class Callback {

		public abstract void onSucess(Object resp);

		public abstract void onError(Object resp);
	}

}
