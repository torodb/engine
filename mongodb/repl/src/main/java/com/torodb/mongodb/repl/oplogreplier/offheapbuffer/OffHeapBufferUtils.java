/*
 * ToroDB
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.torodb.mongodb.repl.oplogreplier.offheapbuffer;

import akka.NotUsed;
import akka.stream.javadsl.Flow;

import com.torodb.akka.chronicle.queue.ChronicleQueueStreamFactory;
import com.torodb.akka.chronicle.queue.Excerpt;
import com.torodb.core.logging.DefaultLoggerFactory;
import com.torodb.mongodb.repl.oplogreplier.OplogBatchMarshaller;
import com.torodb.mongodb.repl.oplogreplier.batch.OplogBatch;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;

import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.LinkedList;
import java.util.Queue;

public class OffHeapBufferUtils {

  private static final Logger logger = DefaultLoggerFactory.get(OffHeapBufferUtils.class);
  private static final String CQ_EXT = "cq4";

  public static Flow<OplogBatch, OplogBatch, NotUsed> createOffheapBuffer(
      OffHeapBufferConfig offHeapConfig) {
    if (offHeapConfig.getEnabled()) {
      logger.debug("OffHeap Buffer enabled, path: " + offHeapConfig.getPath());
      logger.debug("OffHeap Buffer roll cycle: " + offHeapConfig.getRollCycle());

      ChronicleQueue scq = getSingleChronicleQueue(offHeapConfig);

      return Flow.of(OplogBatch.class)
          .via(
              new ChronicleQueueStreamFactory<>()
                  .withQueue(scq)
                  .autoManaged()
                  .createBuffer(new OplogBatchMarshaller()))
          .map(Excerpt::getElement);
    } else {
      logger.trace("OffHeap Buffer disabled");
      return Flow.of(OplogBatch.class);
    }
  }

  private static ChronicleQueue getSingleChronicleQueue(OffHeapBufferConfig offHeapConfig) {

    Path path = createPath(offHeapConfig.getPath());

    StoreFileListener sl = getStoreFileListener(offHeapConfig.getMaxFiles());

    return SingleChronicleQueueBuilder.binary(path)
        .rollCycle(offHeapConfig.getRollCycle().asCqRollCycle())
        .storeFileListener(sl)
        .build();
  }

  private static StoreFileListener getStoreFileListener(int maxFiles) {
    return new StoreFileListener() {
      Queue<File> fileQueue = new LinkedList<>();

      @Override
      public void onReleased(int i, File file) {
        fileQueue.add(file);
        if (fileQueue.size() >= maxFiles) {
          boolean delete = fileQueue.remove().delete();
        }
      }
    };
  }

  private static Path createPath(String offPath) {
    Path path;
    try {
      if (null == offPath || "".equalsIgnoreCase(offPath)) {
        path = Files.createTempDirectory("cq-akka-test");
      } else {
        path = Paths.get(offPath);
        //Remove previous files if exist so there is no previous garbage
        deleteFolder(path);
      }
      deleteOnClose(path);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
    return path;
  }

  private static void deleteOnClose(Path path) {
    Runnable runnable = () -> deleteFolder(path);
    Runtime.getRuntime().addShutdownHook(new Thread(runnable, "deleteOnClose-" + path.toString()));
  }

  @SuppressWarnings("checkstyle:EmptyCatchBlock")
  private static void deleteFolder(Path path) {
    try {
      Files.walkFileTree(
          path,
          new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                throws IOException {
              //Only remove chronicle-queue files
              if (isCqFile(file)) {
                Files.delete(file);
              }
              return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc)
                throws IOException {
              Files.delete(dir);
              return FileVisitResult.CONTINUE;
            }
          });
    } catch (IOException ignored) {
    }
  }

  private static boolean isCqFile(Path file) {

    if (null != file && null != file.toString()) {
      String ext = com.google.common.io.Files.getFileExtension(file.toString());
      if (null != ext && CQ_EXT.equalsIgnoreCase(ext)) {
        return true;
      }
    }
    return false;
  }
}
