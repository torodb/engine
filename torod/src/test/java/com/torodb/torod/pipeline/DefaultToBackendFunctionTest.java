package com.torodb.torod.pipeline;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.torodb.core.TableRefFactory;
import com.torodb.core.backend.WriteBackendTransaction;
import com.torodb.core.d2r.CollectionData;
import com.torodb.core.d2r.DocPartData;
import com.torodb.core.dsl.backend.*;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.impl.TableRefFactoryImpl;
import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.metainf.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockSettings;
import org.mockito.internal.creation.MockSettingsImpl;

import static org.junit.Assert.*;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

/**
 *
 * @author gortiz
 */
public class DefaultToBackendFunctionTest {

    private final TableRefFactory tableRefFactory = new TableRefFactoryImpl();
    private BackendTransactionJobFactory factory;
    private ImmutableMetaCollection collection = new ImmutableMetaCollection("aColName", "aColId", Collections.emptyList());
    private MetaDatabase database = new ImmutableMetaDatabase("aDb", "aId", Collections.singletonList(collection));
    private DefaultToBackendFunction fun;

    public DefaultToBackendFunctionTest() {
    }

    @Before
    public void setUp() {
        factory = spy(new MockBackendConnectionJobFactory());
        fun = new DefaultToBackendFunction(factory, database, collection);
    }

    @Test
    public void testApply() {
        MockSettings settings = new MockSettingsImpl().defaultAnswer((t) -> {
            throw new AssertionError("Method " + t.getMethod() + " was not expected to be called");
        });

        CollectionData collectionData = mock(CollectionData.class);
        
        BatchMetaDocPart allCreatedDocPart = mock(BatchMetaDocPart.class, settings);
        doReturn(false)
                .when(allCreatedDocPart).isCreatedOnCurrentBatch();
        doReturn(Collections.emptyList())
                .when(allCreatedDocPart).getOnBatchModifiedMetaFields();
        DocPartData allCreatedData = mock(DocPartData.class);
        given(allCreatedData.getMetaDocPart())
                .willReturn(
                       allCreatedDocPart 
                );

        BatchMetaDocPart withNewColumnsDocPart = mock(BatchMetaDocPart.class, settings);
        doReturn(false)
                .when(withNewColumnsDocPart).isCreatedOnCurrentBatch();
        doReturn(Lists.newArrayList(new ImmutableMetaField("newFieldName", "newFieldId", FieldType.INTEGER)))
                .when(withNewColumnsDocPart).getOnBatchModifiedMetaFields();
        DocPartData withNewData = mock(DocPartData.class);
        given(withNewData.getMetaDocPart())
                .willReturn(
                       withNewColumnsDocPart
                );

        BatchMetaDocPart withNewScalarDocPart = mock(BatchMetaDocPart.class, settings);
        doReturn(false)
                .when(withNewScalarDocPart).isCreatedOnCurrentBatch();
        doReturn(Lists.newArrayList(new ImmutableMetaScalar("newScalarId", FieldType.INTEGER)))
                .when(withNewScalarDocPart).getOnBatchModifiedMetaScalars();
        DocPartData withNewScalar = mock(DocPartData.class);
        given(withNewData.getMetaDocPart())
                .willReturn(
                       withNewScalarDocPart
                );
        
        BatchMetaDocPart allNewDocPart = mock(BatchMetaDocPart.class, settings);
        doReturn(true)
                .when(allNewDocPart).isCreatedOnCurrentBatch();
        doReturn(Lists.newArrayList(Lists.newArrayList(new ImmutableMetaField("newFieldName", "newFieldId", FieldType.BOOLEAN))).stream())
                .when(allNewDocPart).streamFields();
        doReturn(Lists.newArrayList(new ImmutableMetaScalar("newScalarId", FieldType.BOOLEAN)))
                .when(allNewDocPart).getOnBatchModifiedMetaScalars();
        DocPartData allNewData = mock(DocPartData.class);
        given(allNewData.getMetaDocPart())
                .willReturn(
                       allNewDocPart
                );

        given(collectionData.iterator())
                .willReturn(
                        Lists.newArrayList(allCreatedData, withNewData, allNewData)
                        .iterator()
                );

        //when
        Iterable<BackendTransactionJob> result = fun.apply(collectionData);
        ArrayList<BackendTransactionJob> resultList = Lists.newArrayList(result);

        //then
        assertEquals("Expected 6 jobs to do, but " + resultList.size() +" were recived", 6, resultList.size());

        {
            //jobs created from allCreatedDocPart
            Optional<BackendTransactionJob> insertJob = resultList.stream()
                    .filter((job) -> job instanceof InsertBackendJob
                            && ((InsertBackendJob) job).getDataToInsert()
                                    .equals(allCreatedData))
                    .findAny();
            assertTrue(insertJob.isPresent());
        }
        {
            //jobs created from withNewColumnsDocPart
            Optional<BackendTransactionJob> insertJob = resultList.stream()
                    .filter((job) -> job instanceof InsertBackendJob
                            && ((InsertBackendJob) job).getDataToInsert()
                                    .equals(withNewData))
                    .findAny();
            assertTrue(insertJob.isPresent());
            Optional<BackendTransactionJob> addFieldJob = resultList.stream()
                    .filter((job) -> {
                        if (!(job instanceof AddFieldDDLJob)) {
                            return false;
                        }
                        AddFieldDDLJob castedJob = (AddFieldDDLJob) job;
                        return castedJob.getDocPart().equals(withNewColumnsDocPart)
                                && castedJob.getField().getName().equals("newFieldName")
                                && castedJob.getField().getIdentifier().equals("newFieldId");
                    })
                    .findAny();
            assertTrue(addFieldJob.isPresent());
            
            int addFieldIndex = resultList.indexOf(addFieldJob.get());
            int insertIndex = resultList.indexOf(insertJob.get());
            assert addFieldIndex >= 0;
            assert insertIndex >= 0;
            assertTrue("For a given doc part, all related add fields jobs must be executed before insert "
                    + "jobs, but in this case the add field job has index " + addFieldIndex
                    + " and the insert job has index " + insertIndex,
                    addFieldIndex < insertIndex);
        }

        {
            //jobs created from withNewScalarDocPart
            Optional<BackendTransactionJob> insertJob = resultList.stream()
                    .filter((job) -> job instanceof InsertBackendJob
                            && ((InsertBackendJob) job).getDataToInsert()
                                    .equals(withNewScalar))
                    .findAny();
            assertTrue(insertJob.isPresent());
            Optional<BackendTransactionJob> addScalarJob = resultList.stream()
                    .filter((job) -> {
                        if (!(job instanceof AddScalarDDLJob)) {
                            return false;
                        }
                        AddScalarDDLJob castedJob = (AddScalarDDLJob) job;
                        return castedJob.getDocPart().equals(withNewScalar)
                                && castedJob.getScalar().getIdentifier().equals("newScalarId")
                                && castedJob.getScalar().getType().equals(FieldType.INTEGER);
                    })
                    .findAny();
            assertTrue(addScalarJob.isPresent());

            int addScalarIndex = resultList.indexOf(addScalarJob.get());
            int insertIndex = resultList.indexOf(insertJob.get());
            assert addScalarIndex >= 0;
            assert insertIndex >= 0;
            assertTrue("For a given doc part, all related add scalar jobs must be executed before insert "
                    + "jobs, but in this case the add scalr job has index " + addScalarIndex
                    + " and the insert job has index " + insertIndex,
                    addScalarIndex < insertIndex);
        }

        {
            //jobs created from allNewDocPart
            Optional<BackendTransactionJob> insertJob = resultList.stream()
                    .filter((job) -> job instanceof InsertBackendJob
                            && ((InsertBackendJob) job).getDataToInsert()
                                    .equals(allNewData))
                    .findAny();
            assertTrue(insertJob.isPresent());
            Optional<BackendTransactionJob> addFieldJob = resultList.stream()
                    .filter((job) -> {
                        if (!(job instanceof AddFieldDDLJob)) {
                            return false;
                        }
                        AddFieldDDLJob castedJob = (AddFieldDDLJob) job;
                        return castedJob.getDocPart().equals(allNewDocPart)
                                && castedJob.getField().getName().equals("newFieldName")
                                && castedJob.getField().getIdentifier().equals("newFieldId");
                    })
                    .findAny();
            assertTrue(addFieldJob.isPresent());
            Optional<BackendTransactionJob> createDocPartJob = resultList.stream()
                    .filter((job) -> {
                        if (!(job instanceof AddDocPartDDLJob)) {
                            return false;
                        }
                        AddDocPartDDLJob castedJob = (AddDocPartDDLJob) job;
                        return castedJob.getDocPart().equals(allNewDocPart);
                    })
                    .findAny();
            assertTrue(createDocPartJob.isPresent());

            int createDocPartIndex = resultList.indexOf(createDocPartJob.get());
            int addFieldIndex = resultList.indexOf(addFieldJob.get());
            int insertIndex = resultList.indexOf(insertJob.get());
            assert createDocPartIndex >= 0;
            assert addFieldIndex >= 0;
            assert insertIndex >= 0;
            assertTrue("For a given doc part, all related add fields jobs must be executed before insert "
                    + "jobs, but in this case the add field job has index " + addFieldIndex
                    + " and the insert job has index " + insertIndex,
                    addFieldIndex < insertIndex);
            assertTrue("For a given doc part, all related create doc part jobs must be executed "
                    + "before add field jobs, but in this case the create doc part job has index "
                    + createDocPartIndex + " and "
                    + "the add field job has index " + addFieldIndex,
                    createDocPartIndex < addFieldIndex);
        }
    }

    @Test
    public void testNoBatchMetaDocPart() {
        CollectionData collectionData = mock(CollectionData.class);

        DocPartData data1 = mock(DocPartData.class);
        given(data1.getMetaDocPart())
                .willReturn(
                        new WrapperMutableMetaDocPart(
                                new ImmutableMetaDocPart(tableRefFactory.createRoot(), "aDocPartName"),
                                (o) -> {
                        }
                        )
                );

        given(collectionData.iterator())
                .willReturn(
                        Collections.singleton(data1)
                        .iterator()
                );

        //when
        try {
            fun.apply(collectionData);

            //then
            fail("An exception was expected when a metadoc part which is not a " + BatchMetaDocPart.class + " is used");
        } catch(AssertionError | ClassCastException ex) {
        }
    }

    @Test
    public void testApplyEmpty() {
        CollectionData collectionData = mock(CollectionData.class);
        given(collectionData.iterator())
                .willReturn(Collections.<DocPartData>emptyList().iterator());

        //when
        Iterable<BackendTransactionJob> resultIterable = fun.apply(collectionData);

        //then
        assertTrue("An empty iterator was expected", Iterables.isEmpty(resultIterable));
    }

    private static class MockBackendConnectionJobFactory implements BackendTransactionJobFactory {

        @Override
        public AddDatabaseDDLJob createAddDatabaseDDLJob(MetaDatabase db) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public AddCollectionDDLJob createAddCollectionDDLJob(MetaDatabase db, MetaCollection col) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public AddDocPartDDLJob createAddDocPartDDLJob(MetaDatabase db, MetaCollection col, MetaDocPart docPart) {
            return new AddDocPartDDLJob() {
                @Override
                public MetaDatabase getDatabase() {
                    return db;
                }

                @Override
                public MetaCollection getCollection() {
                    return col;
                }

                @Override
                public MetaDocPart getDocPart() {
                    return docPart;
                }

                @Override
                public void execute(WriteBackendTransaction connection) throws RollbackException {
                    throw new UnsupportedOperationException("Not supported yet.");
                }
            };
        }

        @Override
        public AddFieldDDLJob createAddFieldDDLJob(MetaDatabase db, MetaCollection col, MetaDocPart docPart, MetaField field) {
            return new AddFieldDDLJob() {
                @Override
                public MetaDatabase getDatabase() {
                    return db;
                }

                @Override
                public MetaCollection getCollection() {
                    return col;
                }

                @Override
                public MetaDocPart getDocPart() {
                    return docPart;
                }

                @Override
                public MetaField getField() {
                    return field;
                }

                @Override
                public void execute(WriteBackendTransaction connection) throws RollbackException {
                    throw new UnsupportedOperationException("Not supported yet.");
                }
            };
        }

        @Override
        public InsertBackendJob insert(MetaDatabase db, MetaCollection col, DocPartData data) {
            return new InsertBackendJob() {
                @Override
                public MetaDatabase getDatabase() {
                    return db;
                }

                @Override
                public MetaCollection getCollection() {
                    return col;
                }

                @Override
                public DocPartData getDataToInsert() {
                    return data;
                }

                @Override
                public void execute(WriteBackendTransaction connection) throws RollbackException {
                    throw new UnsupportedOperationException("Not supported yet.");
                }
            };
        }

        @Override
        public AddScalarDDLJob createAddScalarDDLJob(MetaDatabase db, MetaCollection col, MetaDocPart docPart, MetaScalar scalar) {
            return new AddScalarDDLJob() {
                @Override
                public MetaDatabase getDatabase() {
                    return db;
                }

                @Override
                public MetaCollection getCollection() {
                    return col;
                }

                @Override
                public MetaDocPart getDocPart() {
                    return docPart;
                }

                @Override
                public MetaScalar getScalar() {
                    return scalar;
                }

                @Override
                public void execute(WriteBackendTransaction connection) throws UserException {
                    throw new UnsupportedOperationException("Not supported yet.");
                }
            };
        }

    }
}
