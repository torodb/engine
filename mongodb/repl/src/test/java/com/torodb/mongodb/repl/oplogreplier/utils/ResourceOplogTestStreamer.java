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

package com.torodb.mongodb.repl.oplogreplier.utils;




import com.google.common.base.Charsets;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 */
public abstract class ResourceOplogTestStreamer implements Supplier<Stream<OplogApplierTest>> {

  protected abstract Stream<String> streamFileNames();

  @Override
  public Stream<OplogApplierTest> get() {
    return streamFileNames()
        .map(this::fromExtendedJsonResource);
  }

  protected OplogApplierTest fromExtendedJsonResource(String resourceName) {
    String text;
    try (
        InputStream resourceAsStream = ResourceOplogTestStreamer.class.getResourceAsStream(resourceName);
        BufferedReader reader = new BufferedReader(
            new InputStreamReader(resourceAsStream, Charsets.UTF_8))) {
      text = reader.lines().collect(Collectors.joining("\n"));
    } catch (IOException ex) {
      throw new RuntimeException("It was impossible to read the resource " + resourceName + " from "
          + "class " + this.getClass().getTypeName());
    }
    OplogApplierTest test = OplogTestParser.fromExtendedJsonString(text);
    if (!test.getTestName().isPresent()) {
      test = createOplogApplierTest(test, resourceName);
    }

    return test;
  }

  protected OplogApplierTest createOplogApplierTest(OplogApplierTest decorated, String newName) {
    return new OverrideNameOplogTest(decorated, newName);
  }

  protected static class OverrideNameOplogTest implements OplogApplierTest {
    private final OplogApplierTest decorated;
    private final String newName;

    public OverrideNameOplogTest(OplogApplierTest decorated, String newName) {
      this.decorated = decorated;
      this.newName = newName;
    }

    @Override
    public Optional<String> getTestName() {
      return Optional.of(newName);
    }

    @Override
    public boolean shouldIgnore() {
      return decorated.shouldIgnore();
    }

    @Override
    public void execute(Context context) throws Exception {
      decorated.execute(context);
    }
  }

}
