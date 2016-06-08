package com.torodb.backend.d2r;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Stopwatch;
import com.torodb.backend.IdentifierFactory;
import com.torodb.backend.util.InMemoryRidGenerator;
import com.torodb.backend.util.SimpleDocumentFeed;

import static com.torodb.backend.util.MetaInfoOperation.executeMetaOperation;
import static com.torodb.backend.util.TestDataFactory.*;

import com.torodb.core.d2r.D2RTranslator;
import com.torodb.core.d2r.DocPartData;
import com.torodb.metainfo.cache.mvcc.MvccMetainfoRepository;

public class SimpleDocumentTraslationStackStress {

	public static void main(String[] args) {
		
		MvccMetainfoRepository mvccMetainfoRepository = new MvccMetainfoRepository(InitialView);
		
		AtomicLong cont=new AtomicLong(0);
		Stopwatch timer = Stopwatch.createUnstarted();
		SimpleDocumentFeed feed = new SimpleDocumentFeed(1000000);
		feed.getFeed("BenchmarkDoc.json").forEach(doc -> {
			timer.start();
			
			executeMetaOperation(mvccMetainfoRepository, (mutableSnapshot)->{
				D2RTranslator translator = new D2RTranslatorStack(new IdentifierFactory(), new InMemoryRidGenerator(), mutableSnapshot, DB1, COLL1);
				translator.translate(doc);
				for (DocPartData table : translator.getCollectionDataAccumulator()) {
					cont.addAndGet(table.rowCount());
				}
			});
			
			timer.stop();
		});

		long elapsed = timer.elapsed(TimeUnit.MICROSECONDS);
		
		System.out.println("Readed:     " + feed.datasize / (1024 * 1024) + " MBytes");
		System.out.println("Documents:  " + feed.documents);
		System.out.println("Rows:  " + cont);
		System.out.println("Time toro: " + elapsed + " microsecs");
		System.out.println("Speed: " + ((double)elapsed / feed.documents) + " microsecs per document");
		System.out.println("DPS: " + (feed.documents / (double)elapsed * 1000000) + " documents per second");
	}
}
