/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.milvus.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;

import com.google.protobuf.ByteString;

import io.milvus.grpc.BoolArray;
import io.milvus.grpc.DataType;
import io.milvus.grpc.DoubleArray;
import io.milvus.grpc.FieldData;
import io.milvus.grpc.FloatArray;
import io.milvus.grpc.IDs;
import io.milvus.grpc.IntArray;
import io.milvus.grpc.JSONArray;
import io.milvus.grpc.LongArray;
import io.milvus.grpc.MsgBase;
import io.milvus.grpc.MsgType;
import io.milvus.grpc.ScalarField;
import io.milvus.grpc.SparseFloatArray;
import io.milvus.grpc.StringArray;
import io.milvus.grpc.VectorField;

import milvus.proto.msg.Msg.CreateCollectionRequest;
import milvus.proto.msg.Msg.DeleteRequest;
import milvus.proto.msg.Msg.DropCollectionRequest;
import milvus.proto.msg.Msg.InsertRequest;
import milvus.proto.msg.Msg.TimeTickMsg;

/**
 * Builds real protobuf- and msgpack-encoded Milvus MQ messages for the
 * integration tests, using the same {@code milvus-sdk-java} generated classes
 * the runtime deserializer consumes.
 *
 * <p>
 * All factory methods emit the raw wire payload ({@code byte[]} of the
 * {@code msg.Msg} record) so tests can publish them directly to Kafka and
 * then assert the deserialized {@code MilvusChangeEvent}s.
 * </p>
 *
 * <p>
 * Note: Milvus {@code TimeTickMsg} has only a {@code MsgBase} header — it
 * carries no shard/vchannel field at the proto level — so the deserializer
 * attributes timeticks to the whole pchannel. Insert messages optionally
 * carry a shard{(Name} which becomes the vchannel when non-empty; if blank
 * the vchannel falls back to the pchannel (Kafka topic), which is what the
 * integration tests need for the watermark to advance correctly.
 * </p>
 */
public final class ProtoMessageFactory {

    private ProtoMessageFactory() {
    }

    // -- creators ----------------------------------------------------------

    private static MsgBase base(MsgType type, long timestamp) {
        return MsgBase.newBuilder()
                .setMsgType(type)
                .setTimestamp(timestamp)
                .build();
    }

    private static FieldData longField(String name, long... values) {
        LongArray.Builder b = LongArray.newBuilder();
        for (long v : values) {
            b.addData(v);
        }
        return FieldData.newBuilder()
                .setFieldName(name)
                .setType(DataType.Int64)
                .setScalars(ScalarField.newBuilder().setLongData(b.build()))
                .build();
    }

    private static FieldData intField(String name, int... values) {
        IntArray.Builder b = IntArray.newBuilder();
        for (int v : values) {
            b.addData(v);
        }
        return FieldData.newBuilder()
                .setFieldName(name)
                .setType(DataType.Int32)
                .setScalars(ScalarField.newBuilder().setIntData(b.build()))
                .build();
    }

    private static FieldData stringField(String name, String... values) {
        StringArray.Builder b = StringArray.newBuilder();
        for (String v : values) {
            b.addData(v);
        }
        return FieldData.newBuilder()
                .setFieldName(name)
                .setType(DataType.VarChar)
                .setScalars(ScalarField.newBuilder().setStringData(b.build()))
                .build();
    }

    private static FieldData floatField(String name, float... values) {
        FloatArray.Builder b = FloatArray.newBuilder();
        for (float v : values) {
            b.addData(v);
        }
        return FieldData.newBuilder()
                .setFieldName(name)
                .setType(DataType.Float)
                .setScalars(ScalarField.newBuilder().setFloatData(b.build()))
                .build();
    }

    private static FieldData doubleField(String name, double... values) {
        DoubleArray.Builder b = DoubleArray.newBuilder();
        for (double v : values) {
            b.addData(v);
        }
        return FieldData.newBuilder()
                .setFieldName(name)
                .setType(DataType.Double)
                .setScalars(ScalarField.newBuilder().setDoubleData(b.build()))
                .build();
    }

    private static FieldData boolField(String name, boolean... values) {
        BoolArray.Builder b = BoolArray.newBuilder();
        for (boolean v : values) {
            b.addData(v);
        }
        return FieldData.newBuilder()
                .setFieldName(name)
                .setType(DataType.Bool)
                .setScalars(ScalarField.newBuilder().setBoolData(b.build()))
                .build();
    }

    private static FieldData jsonField(String name, String... jsonValues) {
        JSONArray.Builder b = JSONArray.newBuilder();
        for (String v : jsonValues) {
            b.addData(ByteString.copyFromUtf8(v));
        }
        return FieldData.newBuilder()
                .setFieldName(name)
                .setType(DataType.JSON)
                .setScalars(ScalarField.newBuilder().setJsonData(b.build()))
                .build();
    }

    private static FieldData floatVectorField(String name, int dim, float... values) {
        FloatArray.Builder b = FloatArray.newBuilder();
        for (float v : values) {
            b.addData(v);
        }
        return FieldData.newBuilder()
                .setFieldName(name)
                .setType(DataType.FloatVector)
                .setVectors(VectorField.newBuilder()
                        .setDim(dim)
                        .setFloatVector(b.build()))
                .build();
    }

    private static FieldData sparseFloatVectorField(String name, long[] idx, float[] val) {
        ByteBuffer buf = ByteBuffer.allocate(idx.length * 8).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < idx.length; i++) {
            buf.putInt((int) idx[i]);
            buf.putFloat(val[i]);
        }
        buf.flip();
        SparseFloatArray sparse = SparseFloatArray.newBuilder()
                .addContents(ByteString.copyFrom(buf))
                .build();
        return FieldData.newBuilder()
                .setFieldName(name)
                .setType(DataType.SparseFloatVector)
                .setVectors(VectorField.newBuilder().setSparseFloatVector(sparse))
                .build();
    }

    // -- public builders (proto_single wire format) -----------------------

    /**
     * Build a {@code proto_single} InsertRequest payload with the given fields
     * and an explicit row count. The vchannel is intentionally left blank so
     * the deserializer attributes events to the pchannel (Kafka topic).
     */
    public static byte[] insert(String collection, long tso, int numRows, FieldData... fields) {
        InsertRequest.Builder b = InsertRequest.newBuilder()
                .setBase(base(MsgType.Insert, tso))
                .setCollectionName(collection)
                .setNumRows(numRows);
        for (FieldData fd : fields) {
            b.addFieldsData(fd);
        }
        return b.build().toByteArray();
    }

    /** Single-row Insert with id (long), title (string) fields. vchannel blank → pchannel. */
    public static byte[] insertSimpleRow(String collection, long tso, long id, String title) {
        return insert(collection, tso, 1,
                longField("id", id),
                stringField("title", title));
    }

    /** Insert with shard (vchannel) set so multiple vchannels can be exercised. */
    public static byte[] insertWithShard(String collection, String shardName, long tso, long id) {
        return InsertRequest.newBuilder()
                .setBase(base(MsgType.Insert, tso))
                .setCollectionName(collection)
                .setShardName(shardName)
                .setNumRows(1)
                .addFieldsData(longField("id", id))
                .build().toByteArray();
    }

    /** Insert carrying all Milvus scalar/vector field types used by the test suite. */
    public static byte[] insertAllFieldTypes(String collection, long tso, long id, String title,
                                             float[] vector) {
        InsertRequest.Builder b = InsertRequest.newBuilder()
                .setBase(base(MsgType.Insert, tso))
                .setCollectionName(collection)
                .setNumRows(1)
                .addFieldsData(longField("id", id))
                .addFieldsData(intField("count", 7))
                .addFieldsData(stringField("title", title))
                .addFieldsData(floatField("price", 1.5f))
                .addFieldsData(doubleField("score", 99.9d))
                .addFieldsData(boolField("active", true))
                .addFieldsData(jsonField("meta", "{\"k\":1}"))
                .addFieldsData(floatVectorField("embedding", vector.length, vector));
        return b.build().toByteArray();
    }

    /** Build a {@code proto_single} DeleteRequest payload carrying int64 primary keys. */
    public static byte[] delete(String collection, long tso, long... pks) {
        LongArray.Builder lb = LongArray.newBuilder();
        for (long pk : pks) {
            lb.addData(pk);
        }
        IDs ids = IDs.newBuilder().setIntId(lb.build()).build();
        return DeleteRequest.newBuilder()
                .setBase(base(MsgType.Delete, tso))
                .setCollectionName(collection)
                .setPrimaryKeys(ids)
                .build().toByteArray();
    }

    /** Build a {@code proto_single} DeleteRequest payload carrying string primary keys. */
    public static byte[] deleteStringPks(String collection, long tso, String... pks) {
        StringArray.Builder sb = StringArray.newBuilder();
        for (String pk : pks) {
            sb.addData(pk);
        }
        IDs ids = IDs.newBuilder().setStrId(sb.build()).build();
        return DeleteRequest.newBuilder()
                .setBase(base(MsgType.Delete, tso))
                .setCollectionName(collection)
                .setPrimaryKeys(ids)
                .build().toByteArray();
    }

    /** Build a {@code proto_single} {@link TimeTickMsg} at the given timestamp. */
    public static byte[] timeTick(long tso) {
        return TimeTickMsg.newBuilder()
                .setBase(base(MsgType.TimeTick, tso))
                .build().toByteArray();
    }

    /** Build a {@code proto_single} CreateCollectionRequest. */
    public static byte[] createCollection(String collection, long tso) {
        return CreateCollectionRequest.newBuilder()
                .setBase(base(MsgType.CreateCollection, tso))
                .setCollectionName(collection)
                .build().toByteArray();
    }

    /** Build a {@code proto_single} DropCollectionRequest. */
    public static byte[] dropCollection(String collection, long tso) {
        return DropCollectionRequest.newBuilder()
                .setBase(base(MsgType.DropCollection, tso))
                .setCollectionName(collection)
                .build().toByteArray();
    }

    // -- msgpack_batch wire format ----------------------------------------

    /**
     * Build a {@code msgpack_batch} payload containing one Insert (1 row),
     * one Delete and a TimeTick. The {@code vchannel} key inside each inner
     * map is set to {@code vchannel} so events match the watermark source.
     */
    public static byte[] msgpackBatch(String pchannel, String vchannel, String collection,
                                      long insertTso, long deleteTso, long timetickTso,
                                      long insertId, long deletePk)
            throws IOException {
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
        packer.packArrayHeader(3);
        packer.packInt(MsgType.Insert.getNumber()); // outer msgType
        packer.packString(pchannel);
        packer.packArrayHeader(3);
        packInsertMap(packer, collection, vchannel, insertTso, insertId);
        packDeleteMap(packer, collection, vchannel, deleteTso, deletePk);
        packTimeTickMap(packer, vchannel, timetickTso);
        packer.close();
        return packer.toByteArray();
    }

    private static void packInsertMap(MessageBufferPacker packer, String collection, String vchannel,
                                      long tso, long id)
            throws IOException {
        packer.packMapHeader(6);
        packer.packString("msgType");
        packer.packInt(MsgType.Insert.getNumber());
        packer.packString("collectionName");
        packer.packString(collection);
        packer.packString("vchannel");
        packer.packString(vchannel);
        packer.packString("ts");
        packer.packLong(tso);
        packer.packString("numRows");
        packer.packInt(1);
        packer.packString("fieldsData");
        packer.packArrayHeader(1);

        // single long column "id" with one row value
        packer.packMapHeader(4);
        packer.packString("fieldName");
        packer.packString("id");
        packer.packString("type");
        packer.packInt(DataType.Int64.getNumber());
        packer.packString("dim");
        packer.packInt(0);
        packer.packString("values");
        packer.packArrayHeader(1);
        packer.packLong(id);
    }

    private static void packDeleteMap(MessageBufferPacker packer, String collection, String vchannel,
                                      long tso, long pk)
            throws IOException {
        packer.packMapHeader(5);
        packer.packString("msgType");
        packer.packInt(MsgType.Delete.getNumber());
        packer.packString("collectionName");
        packer.packString(collection);
        packer.packString("vchannel");
        packer.packString(vchannel);
        packer.packString("ts");
        packer.packLong(tso);
        packer.packString("primaryKeys");
        packer.packArrayHeader(1);
        packer.packLong(pk);
    }

    private static void packTimeTickMap(MessageBufferPacker packer, String vchannel, long tso) throws IOException {
        packer.packMapHeader(4);
        packer.packString("msgType");
        packer.packInt(MsgType.TimeTick.getNumber());
        packer.packString("collectionName");
        packer.packString("");
        packer.packString("vchannel");
        packer.packString(vchannel);
        packer.packString("ts");
        packer.packLong(tso);
    }
}