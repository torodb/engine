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

package com.torodb.core.guice;

import com.google.inject.Exposed;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.torodb.core.Shutdowner;
import com.torodb.core.TableRefFactory;
import com.torodb.core.impl.TableRefFactoryImpl;
import com.torodb.core.logging.DefaultLoggerFactory;
import com.torodb.core.logging.LoggerFactory;
import com.torodb.core.retrier.DefaultRetrier;
import com.torodb.core.retrier.Retrier;

import java.util.concurrent.ThreadFactory;

/**
 * A module that binds core classes (like {@link Retrier} or {@link InternalTransactionManager} to
 * their default values.
 */
public class CoreModule extends EssentialToroModule {

  private final LoggerFactory lifecycleLoggerFactory;

  public CoreModule(LoggerFactory lifecycleLoggerFactory) {
    this.lifecycleLoggerFactory = lifecycleLoggerFactory;
  }
 
  @Override
  protected void configure() {
    expose(TableRefFactory.class);
    expose(Retrier.class);
    exposeEssential(LoggerFactory.class);

    bind(TableRefFactory.class)
        .to(TableRefFactoryImpl.class)
        .asEagerSingleton();

    bind(Retrier.class)
        .toInstance(DefaultRetrier.getInstance());

    bindEssential(LoggerFactory.class)
        .toInstance(DefaultLoggerFactory.getInstance());
  }

  @Provides
  @Exposed
  @Singleton
  protected Shutdowner createShutdowner(ThreadFactory threadFactory) {
    Shutdowner s = new Shutdowner(threadFactory, lifecycleLoggerFactory);

    return s;
  }

}
