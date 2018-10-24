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

package com.torodb.torod.impl.sql.schema;

import com.google.common.base.Preconditions;
import com.torodb.core.transaction.metainf.ChangedElement;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import com.torodb.core.transaction.metainf.MutableMetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaSnapshot;
import com.torodb.core.transaction.metainf.WrapperMutableMetaSnapshot;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

/**
 *
 */
@NotThreadSafe
class TransactionalMetadata {

  @Nonnull
  private Optional<ImmutableMetaSnapshot> current;
  private Optional<TransactionalSnapshot> activeTransaction = Optional.empty();

  public TransactionalMetadata() {
    this.current = Optional.empty();
  }

  public TransactionalMetadata(ImmutableMetaSnapshot lastCommited) {
    this.current = Optional.of(lastCommited);
  }

  public TransactionalSnapshot createTransactionalSnapshot() {
    if (activeTransaction.isPresent()) {
      throw new IllegalStateException("There is an already open transaction");
    }
    TransactionalSnapshot result = new TransactionalSnapshot(
        getSnapshot(), this::setSnapshot, this::closeTransaction);
    activeTransaction = Optional.of(result);
    return result;
  }

  public ImmutableMetaSnapshot getSnapshot() {
    return current
        .orElseThrow(() -> new IllegalStateException("Metadata hasn't been initalized"));
  }

  public void setSnapshot(ImmutableMetaSnapshot newSnapshot) {
    current = Optional.ofNullable(newSnapshot);
  }

  private void closeTransaction(TransactionalSnapshot argTransaction) {
    TransactionalSnapshot actualTransaction = activeTransaction
        .orElseThrow(() -> new IllegalStateException("There is no active transaction"));

    if (actualTransaction != argTransaction) {
      throw new IllegalArgumentException("The given transaction is not the active one");
    }

    activeTransaction = Optional.empty();
  }

  @NotThreadSafe
  static class TransactionalSnapshot implements MutableMetaSnapshot, AutoCloseable {
    private final MutableMetaSnapshot decorate;
    private final Consumer<ImmutableMetaSnapshot> onCommit;
    private final Consumer<TransactionalSnapshot> onClose;
    private boolean closed = false;

    public TransactionalSnapshot(ImmutableMetaSnapshot lastCommited,
        Consumer<ImmutableMetaSnapshot> onCommit, Consumer<TransactionalSnapshot> onClose) {
      this.decorate = new WrapperMutableMetaSnapshot(lastCommited);
      this.onCommit = onCommit;
      this.onClose = onClose;
    }

    /**
     * Confirms all changes recived by this snapshot, so following calls to
     * {@link TransactionalMetadata#createTransactionalSnapshot()} will see them.
     */
    public void commit() {
      Preconditions.checkState(!closed, "This transaction has been closed");
      onCommit.accept(decorate.immutableCopy());
    }

    @Override
    public ImmutableMetaSnapshot getOrigin() {
      Preconditions.checkState(!closed, "This transaction has been closed");
      return decorate.getOrigin();
    }

    @Override
    public MutableMetaDatabase getMetaDatabaseByIdentifier(String dbIdentifier) {
      Preconditions.checkState(!closed, "This transaction has been closed");
      return decorate.getMetaDatabaseByIdentifier(dbIdentifier);
    }

    @Override
    public MutableMetaDatabase getMetaDatabaseByName(String dbName) {
      Preconditions.checkState(!closed, "This transaction has been closed");
      return decorate.getMetaDatabaseByName(dbName);
    }

    @Override
    public Stream<? extends MutableMetaDatabase> streamMetaDatabases() {
      Preconditions.checkState(!closed, "This transaction has been closed");
      return decorate.streamMetaDatabases();
    }

    @Override
    public MutableMetaDatabase addMetaDatabase(String dbName, String dbId) throws
        IllegalArgumentException {
      Preconditions.checkState(!closed, "This transaction has been closed");
      return decorate.addMetaDatabase(dbName, dbId);
    }

    @Override
    public boolean removeMetaDatabaseByName(String dbName) {
      Preconditions.checkState(!closed, "This transaction has been closed");
      return decorate.removeMetaDatabaseByName(dbName);
    }

    @Override
    public boolean removeMetaDatabaseByIdentifier(String dbId) {
      Preconditions.checkState(!closed, "This transaction has been closed");
      return decorate.removeMetaDatabaseByIdentifier(dbId);
    }

    @Override
    public Stream<ChangedElement<MutableMetaDatabase>> streamModifiedDatabases() {
      Preconditions.checkState(!closed, "This transaction has been closed");
      return decorate.streamModifiedDatabases();
    }

    @Override
    public boolean hasChanged() {
      Preconditions.checkState(!closed, "This transaction has been closed");
      return decorate.hasChanged();
    }

    @Override
    public boolean containsMetaDatabaseByName(String dbName) {
      Preconditions.checkState(!closed, "This transaction has been closed");
      return decorate.containsMetaDatabaseByName(dbName);
    }

    @Override
    public boolean containsMetaDatabaseByIdentifier(String dbId) {
      Preconditions.checkState(!closed, "This transaction has been closed");
      return decorate.containsMetaDatabaseByIdentifier(dbId);
    }

    @Override
    public ImmutableMetaSnapshot immutableCopy() {
      Preconditions.checkState(!closed, "This transaction has been closed");
      return decorate.immutableCopy();
    }

    @Override
    public void close() {
      if (!closed) {
        closed = true;
        onClose.accept(this);
      }
    }
  }
}