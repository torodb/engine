package com.torodb.mongodb.repl.oplogreplier.config;

public interface BufferOffHeapConfig {

  Boolean getEnabled();

  String getPath();

  int getMaxSize();

  BufferRollCycle getRollCycle();

}
