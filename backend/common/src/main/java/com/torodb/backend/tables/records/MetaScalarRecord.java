/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */
package com.torodb.backend.tables.records;

import org.jooq.Field;
import org.jooq.Record4;
import org.jooq.Record5;
import org.jooq.Row5;
import org.jooq.impl.UpdatableRecordImpl;

import com.torodb.backend.tables.MetaScalarTable;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;
import com.torodb.core.transaction.metainf.FieldType;

public abstract class MetaScalarRecord<TableRefType> extends UpdatableRecordImpl<MetaScalarRecord<TableRefType>> 
        implements Record5<String, String, TableRefType, FieldType, String> {
// database, name, original_name, last_did
	private static final long serialVersionUID = -1107968478;

    /**
     * Setter for <code>torodb.scalar.database</code>.
     */
    public void setDatabase(String value) {
        setValue(0, value);
    }

    /**
     * Getter for <code>torodb.scalar.database</code>.
     */
    public String getDatabase() {
        return (String) getValue(0);
    }

    /**
     * Setter for <code>torodb.scalar.collection</code>.
     */
    public void setCollection(String value) {
        setValue(1, value);
    }

    /**
     * Getter for <code>torodb.scalar.collection</code>.
     */
    public String getCollection() {
        return (String) getValue(1);
    }

    /**
     * Setter for <code>torodb.scalar.tableRef</code>.
     */
    public void setTableRef(TableRefType value) {
        setValue(2, value);
    }

    /**
     * Getter for <code>torodb.scalar.tableRef</code>.
     */
    public TableRefType getTableRef() {
        return (TableRefType) getValue(2);
    }

    /**
     * Setter for <code>torodb.scalar.type</code>.
     */
    public void setType(FieldType value) {
        setValue(4, value);
    }

    /**
     * Getter for <code>torodb.scalar.type</code>.
     */
    public FieldType getType() {
        return (FieldType) getValue(4);
    }

    /**
     * Setter for <code>torodb.scalar.idenftifier</code>.
     */
    public void setIdentifier(String value) {
        setValue(5, value);
    }

    /**
     * Getter for <code>torodb.scalar.idenftifier</code>.
     */
    public String getIdentifier() {
        return (String) getValue(5);
    }

	// -------------------------------------------------------------------------
	// Primary key information
	// -------------------------------------------------------------------------

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Record4<String, String, String, String> key() {
		return (Record4) super.key();
	}

	// -------------------------------------------------------------------------
	// Record7 type implementation
	// -------------------------------------------------------------------------

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Row5<String, String, TableRefType, FieldType, String> fieldsRow() {
		return (Row5) super.fieldsRow();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Row5<String, String, TableRefType, FieldType, String> valuesRow() {
		return (Row5) super.valuesRow();
	}

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<String> field1() {
        return metaScalarTable.DATABASE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<String> field2() {
        return metaScalarTable.COLLECTION;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<TableRefType> field3() {
        return metaScalarTable.TABLE_REF;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<FieldType> field4() {
        return metaScalarTable.TYPE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<String> field5() {
        return metaScalarTable.IDENTIFIER;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String value1() {
        return getDatabase();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String value2() {
        return getCollection();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TableRefType value3() {
        return getTableRef();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FieldType value4() {
        return getType();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String value5() {
        return getIdentifier();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetaScalarRecord value1(String value) {
        setDatabase(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetaScalarRecord value2(String value) {
        setCollection(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetaScalarRecord value3(TableRefType value) {
        setTableRef(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetaScalarRecord value4(FieldType value) {
        setType(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetaScalarRecord value5(String value) {
        setIdentifier(value);
        return this;
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public abstract MetaScalarRecord values(String database, String collection, TableRefType tableRef, FieldType type, String identifier);

    public MetaScalarRecord values(String database, String collection, TableRef tableRef, FieldType type, String identifier) {
        return values(database, collection, toTableRefType(tableRef), type, identifier);
    }
    
    protected abstract TableRefType toTableRefType(TableRef tableRef);
    
    public abstract TableRef getTableRefValue(TableRefFactory tableRefFactory);

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    private final MetaScalarTable metaScalarTable;
    
    /**
     * Create a detached MetaScalarRecord
     */
    public MetaScalarRecord(MetaScalarTable metaScalarTable) {
        super(metaScalarTable);
        
        this.metaScalarTable = metaScalarTable;
    }
}
